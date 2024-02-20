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
package io.trino.plugin.deltalake.cache;

import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.cache.CacheKeyProvider;

import java.util.Optional;

public class DeltaLakeCacheKeyProvider
        implements CacheKeyProvider
{
    /**
     * Get the cache key of a TrinoInputFile. Returns Optional.empty() if the file is not cacheable.
     */
    @Override
    public Optional<String> getCacheKey(TrinoInputFile delegate)
    {
        // TODO: Consider caching of the Parquet checkpoint files within _delta_log
        if (!delegate.location().path().contains("/_delta_log/")) {
            return Optional.of(delegate.location().path());
        }
        return Optional.empty();
    }
}
