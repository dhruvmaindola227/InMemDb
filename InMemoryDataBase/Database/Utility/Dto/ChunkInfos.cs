using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace InMemoryDataBase.Database.Utility.Dto
{
    internal class ChunkInfos
    {
        public int ChunkSize { get; set; }
        public int NumChunks { get; set; }
        public Dictionary<string, Dictionary<string, List<int>>>[] StringIndexChunks { get; set; } = [];
        public Dictionary<string, Dictionary<string, List<int>>>[] DateIndexChunks { get; set; } = [];
        public Dictionary<string, Dictionary<long, List<int>>>[] NumIndexChunks { get; set; } = [];
        public List<JsonElement>[] VesselChunks { get; set; } = [];
    }
}
