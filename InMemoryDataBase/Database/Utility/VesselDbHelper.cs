using InMemoryDataBase.CommonLib;
using InMemoryDataBase.Database.Utility.Dto;
using System.Text.Json;

namespace InMemoryDataBase.Database.Utility
{
    internal class VesselDbHelper
    {
        public static async Task<ChunkInfos> InitIndexCollectionsForChunks(
            Dictionary<string, IndexType> indexConfig,
            JsonElement rootElement,
            int arrayLength,
            List<JsonElement> vessels,
            Dictionary<string, Dictionary<string, List<int>>> fieldVsFieldValVsRowIdsForStringCols,
            Dictionary<string, Dictionary<string, List<int>>> fieldVsfieldValVsRowIdsForDateVals,
            Dictionary<string, Dictionary<long, List<int>>> fieldVsFieldValVsRowIdsForNumericVals
        )
        {
            const int chunkSize = 10000;
            var numChunks = (arrayLength + chunkSize - 1) / chunkSize;

            // init chunk arrays with their capacity
            // each item in array is the index for a chunk
            var stringIndexChunks = new Dictionary<string, Dictionary<string, List<int>>>[numChunks];
            var intIndexChunks = new Dictionary<string, Dictionary<long, List<int>>>[numChunks];
            var dateIndexChunks = new Dictionary<string, Dictionary<string, List<int>>>[numChunks];
            var vesselChunks = new List<JsonElement>[numChunks];

            Parallel.For(0, numChunks, chunkIndex =>
            {
                vesselChunks[chunkIndex] = new List<JsonElement>();
                stringIndexChunks[chunkIndex] = new Dictionary<string, Dictionary<string, List<int>>>();
                intIndexChunks[chunkIndex] = new Dictionary<string, Dictionary<long, List<int>>>();
                dateIndexChunks[chunkIndex] = new Dictionary<string, Dictionary<string, List<int>>>();

                foreach (var config in indexConfig)
                {
                    switch (config.Value)
                    {
                        case IndexType.String:
                            stringIndexChunks[chunkIndex][config.Key] = new Dictionary<string, List<int>>();
                            break;
                        case IndexType.Integer:
                            intIndexChunks[chunkIndex][config.Key] = new Dictionary<long, List<int>>();
                            break;
                        case IndexType.Date:
                            dateIndexChunks[chunkIndex][config.Key] = new Dictionary<string, List<int>>();
                            break;
                    }
                }
            });

            // Build indexes for chunks
            await BuildIndexesForChunks(indexConfig, rootElement, arrayLength, chunkSize, numChunks, stringIndexChunks, intIndexChunks, dateIndexChunks, vesselChunks).ConfigureAwait(false);

            var totalVesselCount = vesselChunks.Sum(chunk => chunk.Count);
            vessels.Capacity = totalVesselCount;

            // Merge vessel data
            foreach (var chunk in vesselChunks)
                vessels.AddRange(chunk);

            // Parallel index merging
            await Task.Run(() =>
            {
                // Merge string indexes in parallel
                MergeAllStringIndexes(indexConfig, fieldVsFieldValVsRowIdsForStringCols, stringIndexChunks);

                // Merge int indexes in parallel
                MergeAllNumIndexes(indexConfig, fieldVsFieldValVsRowIdsForNumericVals, intIndexChunks);

                // Merge date indexes in parallel
                MergeAllDateIndexes(indexConfig, fieldVsfieldValVsRowIdsForDateVals, dateIndexChunks);
            });

            // Return the chunk info AFTER all processing is complete
            return new ChunkInfos()
            {
                ChunkSize = chunkSize,
                NumChunks = numChunks,
                StringIndexChunks = stringIndexChunks,
                NumIndexChunks = intIndexChunks,
                DateIndexChunks = dateIndexChunks,
                VesselChunks = vesselChunks
            };
        }

        private static void MergeAllDateIndexes(Dictionary<string, IndexType> indexConfig, Dictionary<string, Dictionary<string, List<int>>> fieldVsfieldValVsRowIdsForDateVals, Dictionary<string, Dictionary<string, List<int>>>[] dateIndexChunks)
        {
            Parallel.ForEach(indexConfig.Where(c => c.Value == IndexType.Date), config =>
            {
                var fieldName = config.Key;
                var fieldIndex = fieldVsfieldValVsRowIdsForDateVals[fieldName];

                foreach (var chunkIndex in dateIndexChunks)
                {
                    if (chunkIndex.TryGetValue(fieldName, out var chunkFieldIndex))
                    {
                        foreach (var valueKvp in chunkFieldIndex)
                        {
                            lock (fieldIndex)
                            {
                                if (!fieldIndex.TryGetValue(valueKvp.Key, out var rowIds))
                                {
                                    rowIds = new List<int>(valueKvp.Value.Count);
                                    fieldIndex[valueKvp.Key] = rowIds;
                                }
                                rowIds.AddRange(valueKvp.Value);
                            }
                        }
                    }
                }
            });
        }

        private static void MergeAllNumIndexes(Dictionary<string, IndexType> indexConfig, Dictionary<string, Dictionary<long, List<int>>> fieldVsFieldValVsRowIdsForNumericVals, Dictionary<string, Dictionary<long, List<int>>>[] intIndexChunks)
        {
            Parallel.ForEach(indexConfig.Where(c => c.Value == IndexType.Integer), config =>
            {
                var fieldName = config.Key;
                var fieldIndex = fieldVsFieldValVsRowIdsForNumericVals[fieldName];

                foreach (var chunkIndex in intIndexChunks)
                {
                    if (chunkIndex.TryGetValue(fieldName, out var chunkFieldIndex))
                    {
                        foreach (var valueKvp in chunkFieldIndex)
                        {
                            lock (fieldIndex)
                            {
                                if (!fieldIndex.TryGetValue(valueKvp.Key, out var rowIds))
                                {
                                    rowIds = new List<int>(valueKvp.Value.Count);
                                    fieldIndex[valueKvp.Key] = rowIds;
                                }
                                rowIds.AddRange(valueKvp.Value);
                            }
                        }
                    }
                }
            });
        }

        private static void MergeAllStringIndexes(Dictionary<string, IndexType> indexConfig, Dictionary<string, Dictionary<string, List<int>>> fieldVsFieldValVsRowIdsForStringCols, Dictionary<string, Dictionary<string, List<int>>>[] stringIndexChunks)
        {
            Parallel.ForEach(indexConfig.Where(c => c.Value == IndexType.String), config =>
            {
                var fieldName = config.Key;
                var fieldIndex = fieldVsFieldValVsRowIdsForStringCols[fieldName];

                foreach (var chunkIndex in stringIndexChunks)
                {
                    if (chunkIndex.TryGetValue(fieldName, out var chunkFieldIndex))
                    {
                        foreach (var valueKvp in chunkFieldIndex)
                        {
                            lock (fieldIndex)
                            {
                                if (!fieldIndex.TryGetValue(valueKvp.Key, out var rowIds))
                                {
                                    rowIds = new List<int>(valueKvp.Value.Count);
                                    fieldIndex[valueKvp.Key] = rowIds;
                                }
                                rowIds.AddRange(valueKvp.Value);
                            }
                        }
                    }
                }
            });
        }

        public static async Task BuildIndexesForChunks(Dictionary<string, IndexType> indexConfig, JsonElement rootElement, int arrayLength, int chunkSize, int numChunks, Dictionary<string, Dictionary<string, List<int>>>[] stringIndexChunks, Dictionary<string, Dictionary<long, List<int>>>[] intIndexChunks, Dictionary<string, Dictionary<string, List<int>>>[] dateIndexChunks, List<JsonElement>[] vesselChunks)
        {
            await Task.Run(() =>
            {
                var elementEnumerator = rootElement.EnumerateArray();
                var elements = new JsonElement[arrayLength];
                var elementIndex = 0;

                // Convert to array in a single pass
                foreach (var element in elementEnumerator)
                {
                    elements[elementIndex++] = element;
                }

                // Process chunks in parallel
                Parallel.For(0, numChunks, chunkIndex =>
                {
                    int startIndex = chunkIndex * chunkSize;
                    int endIndex = Math.Min(startIndex + chunkSize, arrayLength);

                    var chunkVessels = vesselChunks[chunkIndex];
                    chunkVessels.Capacity = endIndex - startIndex;

                    for (int i = startIndex; i < endIndex; i++)
                    {
                        var vessel = elements[i];
                        chunkVessels.Add(vessel);

                        // Build indexes for this record
                        BuildIndexesForRecord(vessel, i,
                            stringIndexChunks[chunkIndex],
                            intIndexChunks[chunkIndex],
                            dateIndexChunks[chunkIndex],
                            indexConfig
                        );
                    }
                });
            });
        }

        public static void BuildIndexesForRecord(JsonElement vessel, int recordIndex,
            Dictionary<string, Dictionary<string, List<int>>> stringChunk,
            Dictionary<string, Dictionary<long, List<int>>> intChunk,
            Dictionary<string, Dictionary<string, List<int>>> dateChunk,
            Dictionary<string, IndexType> indexConfig
        )
        {
            foreach (var config in indexConfig)
            {
                var fieldName = config.Key;
                var indexType = config.Value;

                if (vessel.TryGetProperty(fieldName, out var fieldValue) &&
                    fieldValue.ValueKind != JsonValueKind.Null)
                {
                    try
                    {
                        switch (indexType)
                        {
                            case IndexType.String:
                                var stringValue = fieldValue.GetString();
                                if (!string.IsNullOrEmpty(stringValue))
                                {
                                    var fieldIndex = stringChunk[fieldName];
                                    if (!fieldIndex.ContainsKey(stringValue))
                                    {
                                        fieldIndex[stringValue] = new List<int>();
                                    }
                                    fieldIndex[stringValue].Add(recordIndex);
                                }
                                break;

                            case IndexType.Integer:
                                long intValue;
                                if (fieldValue.ValueKind == JsonValueKind.Number)
                                {
                                    intValue = fieldValue.GetInt64();
                                }
                                else if (fieldValue.ValueKind == JsonValueKind.String &&
                                         long.TryParse(fieldValue.GetString(), out var parsedInt))
                                {
                                    intValue = parsedInt;
                                }
                                else
                                {
                                    continue; // Skip invalid values
                                }

                                var intFieldIndex = intChunk[fieldName];
                                if (!intFieldIndex.ContainsKey(intValue))
                                {
                                    intFieldIndex[intValue] = new List<int>();
                                }
                                intFieldIndex[intValue].Add(recordIndex);
                                break;

                            case IndexType.Date:
                                var dateValue = fieldValue.GetString();
                                if (!string.IsNullOrEmpty(dateValue))
                                {
                                    var dateFieldIndex = dateChunk[fieldName];
                                    if (!dateFieldIndex.ContainsKey(dateValue))
                                    {
                                        dateFieldIndex[dateValue] = new List<int>();
                                    }
                                    dateFieldIndex[dateValue].Add(recordIndex);
                                }
                                break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"{ex.Message} - Failed to init index, please check type in index field in appsettings");
                        throw;
                    }
                }
            }
        }
    }
}