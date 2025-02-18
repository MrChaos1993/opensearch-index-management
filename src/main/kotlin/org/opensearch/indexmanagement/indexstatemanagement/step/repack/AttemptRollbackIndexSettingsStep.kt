/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.repack

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.common.settings.Settings
import org.opensearch.index.TieredMergePolicyProvider
import org.opensearch.indexmanagement.indexstatemanagement.step.repack.AttemptUpdateIndexSettingsStep.Companion.PRE_REPACK_EXPUNGE_DELETES_ALLOWED
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.transport.RemoteTransportException

class AttemptRollbackIndexSettingsStep : Step(NAME) {
    private val logger = LogManager.getLogger(javaClass)
    private val info = mutableMapOf<String, Any>()
    private var stepStatus = StepStatus.STARTING

    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index

        val indexSettingUpdated = setIndexSettings(indexName, context)

        if (!indexSettingUpdated) return this

        stepStatus = StepStatus.COMPLETED
        info["message"] = getSuccessMessage(indexName)

        return this
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun setIndexSettings(indexName: String, context: StepContext): Boolean {
        if (!info.containsKey(PRE_REPACK_EXPUNGE_DELETES_ALLOWED) || info[PRE_REPACK_EXPUNGE_DELETES_ALLOWED] == null) return true
        val settingsBuilder = Settings.builder()
        settingsBuilder.put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING.key, info[PRE_REPACK_EXPUNGE_DELETES_ALLOWED] as Double)
        try {
            val updateSettingsRequest = UpdateSettingsRequest().indices(indexName).settings(settingsBuilder.build())
            val response: AcknowledgedResponse =
                context.client.admin().indices().suspendUntil { updateSettings(updateSettingsRequest, it) }

            if (response.isAcknowledged) {
                return true
            }

            // If response is not acknowledged, then add failed info
            val message = getFailedMessage(indexName)
            logger.warn(message)
            stepStatus = StepStatus.FAILED
            info["message"] = message
        } catch (e: RemoteTransportException) {
            handleException(indexName, ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(indexName, e)
        }

        return false
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
        const val NAME = "attempt_rollback_index_settings_step"

        fun getFailedMessage(index: String) = "Failed to rollback index setting with turning off read-only [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully rollback index with turning off read-only [index=$index]"
    }
}
