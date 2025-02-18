/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.repack

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.indices.upgrade.post.UpgradeRequest
import org.opensearch.action.admin.indices.upgrade.post.UpgradeResponse
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.opensearchapi.getUsefulCauseString
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.transport.RemoteTransportException
import java.time.Instant

class AttemptCreateRepackJobStep : Step(NAME) {
    private val logger = LogManager.getLogger(javaClass)
    private val info = mutableMapOf<String, Any>()
    private var stepStatus = StepStatus.STARTING

    @OptIn(DelicateCoroutinesApi::class)
    @Suppress("TooGenericExceptionCaught", "ComplexMethod")
    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index

        logger.info("Starting repack job")
        try {
            val startTime = Instant.now().toEpochMilli()
            val request = UpgradeRequest(indexName)
            var response: UpgradeResponse? = null
            var throwable: Throwable? = null
            // Guess I don't need this coroutine!
            GlobalScope.launch(Dispatchers.IO + CoroutineName("ISM-Repack-$indexName")) {
                try {
                    logger.info("Sending upgrade request")
                    response = context.client.admin().indices().suspendUntil { upgrade(request, it) }
                    if (response?.status == RestStatus.OK) {
                        logger.info(getSuccessMessage(indexName))
                    } else {
                        logger.warn(getFailedMessage(indexName))
                    }
                } catch (t: Throwable) {
                    throwable = t
                }
            }

            while (response == null && (Instant.now().toEpochMilli() - startTime) < FIVE_MINUTES_IN_MILLIS) {
                delay(FIVE_SECONDS_IN_MILLIS)
                throwable?.let { throw it }
            }

            val shadowedResponse = response
            if (shadowedResponse?.let { it.status == RestStatus.OK } != false) {
                stepStatus = StepStatus.COMPLETED
                info["message"] =
                    if (shadowedResponse == null) getSuccessfulCallMessage(indexName) else getSuccessMessage(indexName)
            } else {
                // Otherwise the request to repack encountered some problem
                stepStatus = StepStatus.FAILED

                info["message"] = getFailedMessage(indexName)
                info["status"] = shadowedResponse.status
                info["shard_failures"] = shadowedResponse.shardFailures.map { it.getUsefulCauseString() }
            }
        } catch (e: RemoteTransportException) {
            handleException(indexName, ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(indexName, e)
        }

        return this
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
            stepMetaData = StepMetaData(
                AttemptUpdateIndexSettingsStep.NAME,
                getStepStartTime(currentMetadata).toEpochMilli(),
                stepStatus,
            ),
            transitionTo = null,
            info = info,
        )

    override fun isIdempotent(): Boolean = false

    companion object {
        const val NAME = "attempt_create_repack_job_step"
        const val FIVE_MINUTES_IN_MILLIS = 1000 * 60 * 5 // how long to wait for the force merge request before moving on
        const val FIVE_SECONDS_IN_MILLIS = 1000L * 5L // delay

        fun getFailedMessage(index: String) = "Failed to start repack [index=$index]"
        fun getSuccessfulCallMessage(index: String) = "Successfully called repack [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully completed repack [index=$index]"
    }
}
