using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
namespace InMemoryDataBase.Database.Utility.Dto;
public class QueryCondition
{
    public string Field { get; set; } = string.Empty;
    public string Operator { get; set; } = string.Empty;
    public object Value { get; set; } = string.Empty;
}

public class QueryInfo
{
    public List<QueryCondition> Conditions { get; set; } = new List<QueryCondition>();
    public string Operator { get; set; } = "";
}
