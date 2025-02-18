/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.repack

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.indices.close.CloseIndexRequest
import org.opensearch.action.admin.indices.close.CloseIndexResponse
import org.opensearch.action.admin.indices.open.OpenIndexRequest
import org.opensearch.action.admin.indices.open.OpenIndexResponse
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE
import org.opensearch.common.settings.Settings
import org.opensearch.index.IndexSettings
import org.opensearch.index.TieredMergePolicyProvider
import org.opensearch.index.engine.EngineConfig
import org.opensearch.indexmanagement.indexstatemanagement.action.RepackAction
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.transport.RemoteTransportException

class AttemptUpdateIndexSettingsStep(private val action: RepackAction) : Step(NAME) {
    private val logger = LogManager.getLogger(javaClass)
    private val info = mutableMapOf<String, Any>()
    private var stepStatus = StepStatus.STARTING

    // 1. Get index.merge.enabled, index.merge.policy, index.merge.policy.expunge_deletes_allowed
    // 2. store to context.metadata.info["pre_repack_settings"]
    // 3. create UpdateIndexSettings with SETTING_BLOCKS_WRITE(set readOnly), index.merge.policy.expunge_deletes_allowed=0, and rest of new index settings from job spec
    // 4. if OK, mark as complete

    @Suppress("ReturnCount")
    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val indexSettings = context.clusterService.state().metadata().index(indexName).settings

//        if (indexSettings.get(MergePolicyProvider.INDEX_MERGE_ENABLED) != "true") {
//            handleException(indexName, Exception("Index merge is not enabled"))
//            return this
//        }

        val mergePolicy = IndexSettings.INDEX_MERGE_POLICY.get(indexSettings)
        if (mergePolicy != "default" && mergePolicy != "tiered") {
            handleException(indexName, Exception("Index merge policy is not tiered"))
            return this
        }

        logger.info("Save index setting")
        info[PRE_REPACK_EXPUNGE_DELETES_ALLOWED] =
            TieredMergePolicyProvider.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING.get(indexSettings)
        val indexSettingUpdated = setIndexSettings(indexName, context)

        if (!indexSettingUpdated) {
            logger.info("Update index setting failed")
            return this
        }

        logger.info("Index setting updated")
        stepStatus = StepStatus.COMPLETED
        info["message"] = getSuccessMessage(indexName)

        return this
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun setIndexSettings(indexName: String, context: StepContext): Boolean {
        val settingsBuilder = Settings.builder()
        settingsBuilder.put(SETTING_BLOCKS_WRITE, true)
        settingsBuilder.put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING.key, 0)
        settingsBuilder.put(EngineConfig.INDEX_CODEC_SETTING.key, this.action.newCodec)
        try {
            logger.info("Closing index")
            val closeIndexRequest =
                CloseIndexRequest()
                    .indices(indexName)

            val closeIndexResponse: CloseIndexResponse =
                context.client.admin().indices()
                    .suspendUntil { close(closeIndexRequest, it) }

            if (!closeIndexResponse.isAcknowledged) {
                // If response is not acknowledged, then add failed info
                handleFailedResponse(indexName)
                return false
            }
            logger.info("Index closed")

            logger.info("Update index settings")
            val updateSettingsRequest = UpdateSettingsRequest().indices(indexName).settings(settingsBuilder.build())
            val response: AcknowledgedResponse =
                context.client.admin().indices().suspendUntil { updateSettings(updateSettingsRequest, it) }

            if (!response.isAcknowledged) {
                // If response is not acknowledged, then add failed info
                handleFailedResponse(indexName)
                return false
            }

            logger.info("index settings updated")
            logger.info("Opening index")
            val openIndexRequest =
                OpenIndexRequest()
                    .indices(indexName)

            val openIndexResponse: OpenIndexResponse = context.client.admin().indices().suspendUntil { open(openIndexRequest, it) }
            if (!openIndexResponse.isAcknowledged) {
                // If response is not acknowledged, then add failed info
                handleFailedResponse(indexName)
                return false
            }

            logger.info("Index opened")

            return true
        } catch (e: RemoteTransportException) {
            handleException(indexName, ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(indexName, e)
        }

        return false
    }

    private fun handleFailedResponse(indexName: String) {
        val message = getFailedMessage(indexName)
        logger.warn(message)
        stepStatus = StepStatus.FAILED
        info["message"] = message
    }

    private fun handleException(indexName: String, e: Exception) {
        val message = getFailedMessage(indexName)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        info["message"] = message
        val errorMessage = e.message
        if (errorMessage != null) info["cause"] = errorMessage
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData =
        currentMetadata.copy(
            stepMetaData = StepMetaData(NAME, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info,
        )

    override fun isIdempotent() = true

    companion object {
        const val NAME = "attempt_update_index_settings_step"
        const val PRE_REPACK_EXPUNGE_DELETES_ALLOWED = "pre_repack_expunge_deletes_allowed"

        fun getFailedMessage(index: String) = "Failed to update index setting with switching to read-only [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully update index with switching to read-only [index=$index]"
    }
}
