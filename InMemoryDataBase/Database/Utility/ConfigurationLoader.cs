using InMemoryDataBase.CommonLib;
using Microsoft.Extensions.Configuration;

public static class ConfigurationLoader
{
    public static Dictionary<string, IndexType> LoadIndexConfiguration()
    {
        var configuration = new ConfigurationBuilder().AddJsonFile("./appsettings.json");
        var config = configuration.Build();

        // For your index configuration:
        var indexSection = config.GetSection("Database:IndexFieldVsType");
        var indexConfiguration = new Dictionary<string, IndexType>();

        foreach (var item in indexSection.GetChildren())
        {
            if (Enum.TryParse<IndexType>(item.Value, true, out var indexType))
            {
                indexConfiguration[item.Key] = indexType;
            }
        }
        return indexConfiguration;
    }
}