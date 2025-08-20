using InMemoryDataBase.CommonLib;
using InMemoryDataBase.Database.Utility;
using InMemoryDataBase.Database.Utility.Dto;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace InMemoryDataBase.Database
{
    public class VesselDatabase
    {
        // data
        private readonly List<JsonElement> _vessels;
        // indexes
        private Dictionary<string, Dictionary<string, List<int>>> _fieldVsFieldValVsRowIdsForStringCols;
        private Dictionary<string, Dictionary<long, List<int>>> _fieldVsFieldValVsRowIdsForNumericVals;
        private Dictionary<string, Dictionary<string, List<int>>> _fieldVsfieldValVsRowIdsForDateVals;
        // cache
        private Dictionary<string, List<JsonElement>> _queryStringVsOutput;

        // Index configuration - user defines what to index and how
        private Dictionary<string, IndexType> _fieldVsIndexType;

        public VesselDatabase()
        {
            _vessels = new List<JsonElement>();
            _fieldVsFieldValVsRowIdsForStringCols = new Dictionary<string, Dictionary<string, List<int>>>(StringComparer.OrdinalIgnoreCase);
            _fieldVsFieldValVsRowIdsForNumericVals = new Dictionary<string, Dictionary<long, List<int>>>(StringComparer.OrdinalIgnoreCase);
            _fieldVsfieldValVsRowIdsForDateVals = new Dictionary<string, Dictionary<string, List<int>>>(StringComparer.OrdinalIgnoreCase);
            _queryStringVsOutput = new Dictionary<string, List<JsonElement>>(StringComparer.OrdinalIgnoreCase);
            var indexConfig = ConfigurationLoader.LoadIndexConfiguration();
            // User-defined index configuration (max 3 indexes)
            if (indexConfig.Count > 3)
            {
                throw new ArgumentException("Maximum 3 indexes allowed");
            }

            _fieldVsIndexType = indexConfig;

            foreach (var config in indexConfig)
            {
                switch (config.Value)
                {
                    case IndexType.String:
                        _fieldVsFieldValVsRowIdsForStringCols[config.Key] = new Dictionary<string, List<int>>(StringComparer.OrdinalIgnoreCase);
                        break;
                    case IndexType.Integer:
                        _fieldVsFieldValVsRowIdsForNumericVals[config.Key] = new Dictionary<long, List<int>>();
                        break;
                    case IndexType.Date:
                        _fieldVsfieldValVsRowIdsForDateVals[config.Key] = new Dictionary<string, List<int>>(StringComparer.OrdinalIgnoreCase);
                        break;
                }
            }
        }

        public async Task LoadDataAsync(string jsonFilePath)
        {
            var totalStopwatch = System.Diagnostics.Stopwatch.StartNew();
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            Console.WriteLine("Loading JSON file...");

            var fileInfo = new FileInfo(jsonFilePath);
            Console.WriteLine($"File size: {fileInfo.Length / 1024 / 1024:F1} MB");

            using var fileStream = new FileStream(jsonFilePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 65536); // 64kb buffer
            var jsonDocument = await JsonDocument.ParseAsync(fileStream);

            Console.WriteLine($"File loaded and parsed in {stopwatch.ElapsedMilliseconds}ms");
            stopwatch.Restart();

            var rootElement = jsonDocument.RootElement;
            var arrayLength = rootElement.GetArrayLength();
            _vessels.Capacity = arrayLength; // init capacity to avoid list resizing

            // This method now properly builds the indexes and returns the chunk info
            var chunkInfos = await VesselDbHelper.InitIndexCollectionsForChunks(
                _fieldVsIndexType,
                rootElement,
                arrayLength,
                _vessels,
                _fieldVsFieldValVsRowIdsForStringCols,
                _fieldVsfieldValVsRowIdsForDateVals,
                _fieldVsFieldValVsRowIdsForNumericVals).ConfigureAwait(false);

            Console.WriteLine($"Index building and merging completed in {stopwatch.ElapsedMilliseconds}ms");

            totalStopwatch.Stop();
            Console.WriteLine($"Loaded {_vessels.Count} vessels with {_fieldVsIndexType.Count} indexes in {totalStopwatch.ElapsedMilliseconds}ms total");
        }


        public List<JsonElement> Query(string queryString)
        {
            if (_queryStringVsOutput.TryGetValue(queryString, out var cachedResult))
            {
                Console.WriteLine("Query result retrieved from cache");
                return cachedResult;
            }

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var result = ExecuteQuery(queryString);
            stopwatch.Stop();

            Console.WriteLine($"Query executed in {stopwatch.ElapsedMilliseconds}ms");

            // Cache the result
            _queryStringVsOutput[queryString] = result;

            return result;
        }

        private List<JsonElement> ExecuteQuery(string queryString)
        {
            var query = ParseQuery(queryString);

            if (query is null)
            {
                Console.WriteLine("Invalid query format");
                return new List<JsonElement>();
            }

            List<int> matchingIndices;

            if (query.Conditions.Count == 1)
            {
                matchingIndices = GetMatchingIndicesForCondition(query.Conditions[0]);
            }
            else if (query.Conditions.Count == 2 && query.Operator == "AND")
            {
                var indices1 = GetMatchingIndicesForCondition(query.Conditions[0]);
                var indices2 = GetMatchingIndicesForCondition(query.Conditions[1]);
                matchingIndices = indices1.Intersect(indices2).ToList();
            }
            else
            {
                Console.WriteLine("Query format not supported");
                return new List<JsonElement>();
            }

            return matchingIndices.Select(index => _vessels[index]).ToList();
        }

        private List<int> GetMatchingIndicesForCondition(QueryCondition condition)
        {
            // Check if field is indexed and determine index type
            if (_fieldVsIndexType.TryGetValue(condition.Field, out var indexType))
            {
                switch (indexType)
                {
                    case IndexType.String:
                        return GetStringIndexMatches(condition);
                    case IndexType.Integer:
                        return GetIntIndexMatches(condition);
                    case IndexType.Date:
                        return GetDateIndexMatches(condition);
                }
            }
            return GetIndicesLinearScan(condition);
        }

        private List<int> GetStringIndexMatches(QueryCondition condition)
        {
            var fieldIndex = _fieldVsFieldValVsRowIdsForStringCols[condition.Field];
            var result = new List<int>();
            var queryValue = condition.Value.ToString();
            if (queryValue is null)
                return result;
            switch (condition.Operator)
            {
                case "=":
                    if (fieldIndex.TryGetValue(queryValue, out var exactMatches))
                    {
                        result.AddRange(exactMatches);
                    }
                    break;
                default:
                    return GetIndicesLinearScan(condition);
            }

            return result;
        }

        private List<int> GetIntIndexMatches(QueryCondition condition)
        {
            var fieldIndex = _fieldVsFieldValVsRowIdsForNumericVals[condition.Field];
            var result = new List<int>();

            if (!long.TryParse(condition.Value.ToString(), out var queryValue))
            {
                return result; // Invalid number
            }

            switch (condition.Operator)
            {
                case "=":
                    if (fieldIndex.TryGetValue(queryValue, out var exactMatches))
                    {
                        result.AddRange(exactMatches);
                    }
                    break;

                case "<":
                    foreach (var kvp in fieldIndex)
                    {
                        if (kvp.Key < queryValue)
                        {
                            result.AddRange(kvp.Value);
                        }
                    }
                    break;

                case ">":
                    foreach (var kvp in fieldIndex)
                    {
                        if (kvp.Key > queryValue)
                        {
                            result.AddRange(kvp.Value);
                        }
                    }
                    break;
            }

            return result;
        }

        private List<int> GetDateIndexMatches(QueryCondition condition)
        {
            var fieldIndex = _fieldVsfieldValVsRowIdsForDateVals[condition.Field];
            var result = new List<int>();
            var queryValue = Convert.ToString(condition.Value);
            if (queryValue is null)
                return result;
            switch (condition.Operator)
            {
                case "=":
                    if (fieldIndex.TryGetValue(queryValue, out var exactMatches))
                    {
                        result.AddRange(exactMatches);
                    }
                    break;
                default:
                    return GetIndicesLinearScan(condition);
            }

            return result;
        }

        private List<int> GetIndicesLinearScan(QueryCondition condition)
        {
            var partitioner = Partitioner.Create(0, _vessels.Count);
            var results = new ConcurrentBag<int>();

            Parallel.ForEach(partitioner, range =>
            {
                for (int i = range.Item1; i < range.Item2; i++)
                {
                    var vessel = _vessels[i];
                    if (vessel.TryGetProperty(condition.Field, out var fieldValue) &&
                        EvaluateCondition(fieldValue, condition.Value, condition.Operator))
                    {
                        results.Add(i);  // Thread-safe collection
                    }
                }
            });

            return results.OrderBy(x => x).ToList();
        }

        private static bool EvaluateCondition(JsonElement fieldValue, object queryValue, string op)
        {
            switch (op)
            {
                case "=":
                    return fieldValue.ToString() == queryValue.ToString();
                case "<":
                    if (fieldValue.ValueKind == JsonValueKind.Number &&
                        double.TryParse(queryValue.ToString(), out var qVal))
                    {
                        return fieldValue.GetDouble() < qVal;
                    }
                    break;
                case ">":
                    if (fieldValue.ValueKind == JsonValueKind.Number &&
                        double.TryParse(queryValue.ToString(), out var qVal2))
                    {
                        return fieldValue.GetDouble() > qVal2;
                    }
                    break;
            }
            return false;
        }

        private QueryInfo? ParseQuery(string queryString)
        {
            queryString = queryString.Trim().Replace("WHERE ", "").Trim();

            var query = new QueryInfo();

            if (queryString.Contains(" AND "))
            {
                var parts = queryString.Split(" AND ");
                query.Operator = "AND";
                foreach (var part in parts)
                {
                    var condition = ParseCondition(part.Trim());
                    if (condition != null)
                        query.Conditions.Add(condition);
                }
            }
            else
            {
                var condition = ParseCondition(queryString);
                if (condition != null)
                    query.Conditions.Add(condition);
            }

            return query.Conditions.Count > 0 ? query : null;
        }

        private static QueryCondition? ParseCondition(string conditionString)
        {
            string[] operators = { "=", "<", ">" };

            foreach (var op in operators)
            {
                if (conditionString.Contains(op))
                {
                    var parts = conditionString.Split(op, 2);
                    if (parts.Length == 2)
                    {
                        var field = parts[0].Trim();
                        var value = parts[1].Trim().Trim('\'', '"');

                        return new QueryCondition { Field = field, Operator = op, Value = value };
                    }
                }
            }

            return null;
        }

        public void PrintIndexStats()
        {
            Console.WriteLine("\nIndex Statistics:");

            foreach (var config in _fieldVsIndexType)
            {
                var fieldName = config.Key;
                var indexType = config.Value;

                switch (indexType)
                {
                    case IndexType.String:
                        var stringIndex = _fieldVsFieldValVsRowIdsForStringCols[fieldName];
                        Console.WriteLine($"{fieldName} (String): {stringIndex.Count} unique values");
                        PrintTopValues(stringIndex.OrderByDescending(kvp => kvp.Value.Count).Take(3));
                        break;

                    case IndexType.Integer:
                        var intIndex = _fieldVsFieldValVsRowIdsForNumericVals[fieldName];
                        Console.WriteLine($"{fieldName} (Integer): {intIndex.Count} unique values");
                        PrintTopValues(intIndex.OrderByDescending(kvp => kvp.Value.Count).Take(3));
                        break;

                    case IndexType.Date:
                        var dateIndex = _fieldVsfieldValVsRowIdsForDateVals[fieldName];
                        Console.WriteLine($"{fieldName} (Date): {dateIndex.Count} unique values");
                        PrintTopValues(dateIndex.OrderByDescending(kvp => kvp.Value.Count).Take(3));
                        break;
                }
            }
        }

        private void PrintTopValues<T>(IEnumerable<KeyValuePair<T, List<int>>> topValues)
        {
            foreach (var kvp in topValues)
            {
                Console.WriteLine($"  {kvp.Key}: {kvp.Value.Count} records");
            }
        }

        public void PrintQueryResult(List<JsonElement> results, int maxResults = 5)
        {
            Console.WriteLine($"\nFound {results.Count} results:");

            foreach (var vessel in results.Take(maxResults))
            {
                Console.WriteLine(vessel.GetRawText());
                Console.WriteLine();
            }

            if (results.Count > maxResults)
                Console.WriteLine($"... and {results.Count - maxResults} more results");
        }
    }

}
