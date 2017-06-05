using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Text.RegularExpressions;

public static class Utility
{
    public static long ConvertToLong(JToken value)
    {
        string strValue = value?.ToString();
        if (string.IsNullOrEmpty(strValue))
        {
            return 0;
        }
        else
        {
            return long.Parse(strValue);
        }
    }

    public static long ConvertToLong(string value)
    {
        string strValue = value?.ToString();
        if (string.IsNullOrEmpty(strValue))
        {
            return 0;
        }
        else
        {
            return long.Parse(strValue);
        }
    }

    public static List<string> ExtractHashTag(string message)
    {
        List<string> hashTags = new List<string>();
        if (!string.IsNullOrEmpty(message))
        {
            var matches = Regex.Matches(message, "(\\#\\w+) ");
            foreach (Match match in matches)
            {
                hashTags.Add(match.Value);
            }
        }

        return hashTags;
    }
}
