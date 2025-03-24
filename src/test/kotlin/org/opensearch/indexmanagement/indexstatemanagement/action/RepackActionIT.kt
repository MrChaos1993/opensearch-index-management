/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.settings.Settings
import org.opensearch.index.IndexSettings
import org.opensearch.index.codec.CodecService
import org.opensearch.index.engine.EngineConfig
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class RepackActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test basic workflow`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val newCodec = CodecService.ZLIB
        // Create a Policy with one State that only preforms a repack Action
        val repackActionConfig = RepackAction(newCodec = newCodec, actionIndex = 0)
        val states = listOf(State("RepackState", listOf(repackActionConfig), listOf()))
        val policy =
            Policy(
                id = policyID,
                description = "$testIndexName description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
            )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        // Add sample data to increase segment count, passing in a delay to ensure multiple segments get created
        insertSampleData(indexName, 3, 1000)
        waitFor { assertTrue("Segment count for [$indexName] was less than expected", validateSegmentCount(indexName, min = 2)) }

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Will change the startTime each execution so that it triggers in 2 seconds
        // First execution: Policy is initialized
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Second execution: Index settings is updated before repack
        updateManagedIndexConfigStartTime(managedIndexConfig)
        logger.info("INDEX SETTINGS!!!: {}", getIndexSettings(indexName))
        waitFor {
            assertEquals(newCodec, getIndexSettingsAsMap(indexName).getOrDefault(EngineConfig.INDEX_CODEC_SETTING.key, ""))
        }

        // Third execution: Repack operation is kicked off
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Fourth execution: Waits for repack to complete, which will happen in this execution since index is small
        updateManagedIndexConfigStartTime(managedIndexConfig)
        // Check all index segments has correct mode
        val indexSettingsMap = getIndexSettingsAsMap(indexName)
        val settingsBuilder = Settings.builder().loadFromMap(indexSettingsMap)
        val metadata = IndexMetadata.builder(indexName).settings(settingsBuilder).build()
        val indexSettings = IndexSettings(metadata, Settings.EMPTY)

        val cs = CodecService(null, indexSettings, logger)
        val newCodecOjb = cs.codec(newCodec)
        val storedFieldsFormat = newCodecOjb.storedFieldsFormat()
        val modeKey = storedFieldsFormat.javaClass.getDeclaredField("MODE_KEY")
        val mode = storedFieldsFormat.javaClass.getDeclaredField("mode")
        mode.isAccessible = true

        val expectedKey = modeKey.get(storedFieldsFormat) as String
        val expectedValue = (mode.get(storedFieldsFormat) as Enum<*>).name
        logger.info("Expected key: '{}', Expected value: '{}'", expectedKey, expectedValue)
        waitFor {
            assertTrue(
                "Found segment for [$indexName] after repack with incorrect mode",
                validateSegmentsAttributes(indexName, expectedKey, expectedValue),
            )
        }
    }
}
