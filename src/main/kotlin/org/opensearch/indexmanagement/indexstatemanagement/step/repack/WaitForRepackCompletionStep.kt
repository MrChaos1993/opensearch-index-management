/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.repack

import org.apache.logging.log4j.LogManager
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData

class WaitForRepackCompletionStep : Step(NAME) {
    private val logger = LogManager.getLogger(javaClass)
    private val info = mutableMapOf<String, Any>()
    private var stepStatus = StepStatus.STARTING

    @Suppress("TooGenericExceptionCaught", "ReturnCount")
    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index

//        context.client.pitSegments()
//        context.clusterService.
        context.clusterService.state().metadata().index(indexName).state
        // TODO: check segments->attributes

        logger.warn("Not yet implemented")

        stepStatus = StepStatus.COMPLETED
        info["message"] = AttemptRollbackIndexSettingsStep.getSuccessMessage(indexName)
        return this

        // TODO("Not yet implemented")
    }

//    private fun getActionStartTime(context: StepContext): Instant {
//        val managedIndexMetaData = context.metadata
//        val startTime = managedIndexMetaData.actionMetaData?.startTime ?: return Instant.now()
//
//        return Instant.ofEpochMilli(startTime)
//    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData =
        currentMetadata.copy(
            stepMetaData = StepMetaData(AttemptUpdateIndexSettingsStep.NAME, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info,
        )

    override fun isIdempotent(): Boolean = true

    companion object {
        const val NAME = "wait_for_repack_completion_step"
        const val REPACK_TIMEOUT_IN_SECONDS = 43200L // 12 hours

        fun getFailedTimedOutMessage(index: String) = "Repack timed out [index=$index]"
        fun getFailedSegmentCheckMessage(index: String) = "Failed to check segments when waiting for repack to complete [index=$index]"
        fun getWaitingMessage(index: String) = "Waiting for repack to complete [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully confirmed segments repack [index=$index]"
    }
}
