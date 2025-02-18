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
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.common.collect.Tuple
import org.opensearch.common.settings.Settings
import org.opensearch.index.IndexSettings
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

    @Suppress("ReturnCount")
    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val indexSettings = context.clusterService.state().metadata().index(indexName).settings

        val mergePolicy = IndexSettings.INDEX_MERGE_POLICY.get(indexSettings)
        if (mergePolicy != "default" && mergePolicy != "tiered") {
            handleException(indexName, Exception("Index merge policy is not tiered"))
            return this
        }

        val mode = getAnySegmentMode(context, indexName)
        if (mode == null) {
            logger.error("Failed to get segments for index '$indexName'")
            return this
        }

        info[OLD_CODEC_MODE_KEY] = mode.v1()
        info[OLD_CODEC_MODE_VALUE] = mode.v2()

        logger.info("Saving [$indexName] index setting")
        val indexSettingUpdated = setIndexSettings(indexName, context)

        if (!indexSettingUpdated) {
            logger.error("Update [$indexName] index setting failed")
            return this
        }

        logger.info("Updated [$indexName] index setting")
        stepStatus = StepStatus.COMPLETED
        info["message"] = getSuccessMessage(indexName)

        return this
    }

    private suspend fun getAnySegmentMode(context: StepContext, indexName: String): Tuple<String, String>? {
        val req = IndicesSegmentsRequest(indexName)
        val resp = context.client.admin().indices().suspendUntil { segments(req, it) }
        val indexSegments = resp.indices[indexName]
        if (indexSegments == null) {
            handleFailedResponse(indexName)
            return null
        }

        val attributes = indexSegments.shards.entries.random().value.shards.random().segments.random().attributes

        val modeAttributes = attributes.filterKeys { it.endsWith(".mode") }
        if (modeAttributes.size > 1) {
            logger.warn("For [$indexName] found more than 1 attribute with suffix '.mode': $modeAttributes")
        }

        val selectedModeAttribute = modeAttributes.entries.random()

        logger.info("For [$indexName] index select attribute: $selectedModeAttribute")

        return Tuple.tuple(selectedModeAttribute.key, selectedModeAttribute.value)
    }

    @Suppress("ReturnCount", "TooGenericExceptionCaught")
    private suspend fun setIndexSettings(indexName: String, context: StepContext): Boolean {
        val settingsBuilder = Settings.builder()
        settingsBuilder.put(EngineConfig.INDEX_CODEC_SETTING.key, this.action.newCodec)
        try {
            logger.info("Closing [$indexName] index to update codec setting")
            val closeIndexRequest =
                CloseIndexRequest().indices(indexName)

            val closeIndexResponse: CloseIndexResponse =
                context.client.admin().indices()
                    .suspendUntil { close(closeIndexRequest, it) }

            if (!closeIndexResponse.isAcknowledged) {
                // If response is not acknowledged, then add failed info
                handleFailedResponse(indexName)
                return false
            }
            logger.info("Index [$indexName] closed")

            logger.info("Update [$indexName] index codec setting")
            val updateSettingsRequest = UpdateSettingsRequest().indices(indexName).settings(settingsBuilder.build())
            val response: AcknowledgedResponse =
                context.client.admin().indices().suspendUntil { updateSettings(updateSettingsRequest, it) }

            if (!response.isAcknowledged) {
                // If response is not acknowledged, then add failed info
                handleFailedResponse(indexName)
                return false
            }

            logger.info("Index [$indexName] settings updated")
            logger.info("Opening [$indexName] index after update codec setting")
            val openIndexRequest =
                OpenIndexRequest()
                    .indices(indexName)

            val openIndexResponse: OpenIndexResponse = context.client.admin().indices().suspendUntil { open(openIndexRequest, it) }
            if (!openIndexResponse.isAcknowledged) {
                // If response is not acknowledged, then add failed info
                handleFailedResponse(indexName)
                return false
            }

            logger.info("Index [$indexName] opened")

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
        const val OLD_CODEC_MODE_KEY = "old_codec_mode_key"
        const val OLD_CODEC_MODE_VALUE = "old_codec_mode_value"

        fun getFailedMessage(index: String) = "Failed to update index setting with switching to read-only [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully update index with switching to read-only [index=$index]"
    }
}
