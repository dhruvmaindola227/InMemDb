using InMemoryDataBase.Database;

public class Program
{
    public static async Task Main(string[] args)
    {

        var db = new VesselDatabase();

        var totalStopwatch = System.Diagnostics.Stopwatch.StartNew();
        await db.LoadDataAsync("../../../Data/vessels.json").ConfigureAwait(false);

        totalStopwatch.Stop();

        Console.WriteLine($"\nTotal loading time: {totalStopwatch.ElapsedMilliseconds}ms");

        db.PrintIndexStats();

        Console.WriteLine("\nEnter queries (or 'exit' to quit):");
        Console.WriteLine("  Z13_STATUS_CODE (Integer): WHERE Z13_STATUS_CODE = 4");
        Console.WriteLine("  BUILDER_GROUP (String): WHERE BUILDER_GROUP = Namura Zosensho");

        while (true)
        {
            Console.Write("\nQuery> ");
            var input = Console.ReadLine();

            if (input?.ToLower() == "exit")
                break;

            if (string.IsNullOrWhiteSpace(input))
                continue;

            var results = db.Query(input);
            db.PrintQueryResult(results);
        }
    }
}