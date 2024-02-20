/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DeltaLakeCommitSummary
{
    private final List<MetadataEntry> metadataUpdates;
    private final Optional<ProtocolEntry> protocol;
    private final Optional<Boolean> isBlindAppend;

    public DeltaLakeCommitSummary(List<DeltaLakeTransactionLogEntry> transactionLogEntries)
    {
        requireNonNull(transactionLogEntries, "transactionLogEntries is null");
        ImmutableList.Builder<MetadataEntry> metadataUpdatesBuilder = ImmutableList.builder();
        Optional<ProtocolEntry> optionalProtocol = Optional.empty();
        Optional<CommitInfoEntry> optionalCommitInfo = Optional.empty();

        for (DeltaLakeTransactionLogEntry transactionLogEntry : transactionLogEntries) {
            if (transactionLogEntry.getMetaData() != null) {
                metadataUpdatesBuilder.add(transactionLogEntry.getMetaData());
            }
            else if (transactionLogEntry.getProtocol() != null) {
                optionalProtocol = Optional.of(transactionLogEntry.getProtocol());
            }
            else if (transactionLogEntry.getCommitInfo() != null) {
                optionalCommitInfo = Optional.of(transactionLogEntry.getCommitInfo());
            }
        }

        metadataUpdates = metadataUpdatesBuilder.build();
        protocol = optionalProtocol;
        isBlindAppend = optionalCommitInfo.flatMap(CommitInfoEntry::isBlindAppend);
    }

    public List<MetadataEntry> getMetadataUpdates()
    {
        return metadataUpdates;
    }

    public Optional<ProtocolEntry> getProtocol()
    {
        return protocol;
    }

    public Optional<Boolean> getIsBlindAppend()
    {
        return isBlindAppend;
    }
}
