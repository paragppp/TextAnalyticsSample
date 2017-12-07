using RestSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace TextAnalyticsSample
{
    class Program
    {
        class maledata
        {
            public string account_id;
            public string create_date;
            public string profile;
            public string liked_count;
            public string log_predicted_liked_count;
            public string log_liked_count;
            public string account_id2;
            public string relative_path;
            public string effect;
        }

        static void Main(string[] args)
        {
            string[] read;
            char[] seperators = { '\t' };

            Console.OutputEncoding = Encoding.Unicode;

            // check arguments
            if (args.Length < 4)
            {
                Console.WriteLine("\nUsage:\n\nTextAnalyticsSample.exe <input TSV file path> <output TSV file path> <azure text analytics key> <server location>\n");

                return;
            }

            string input_file = args[0];
            string output_file = args[1];
            string analytics_key = args[2];
            string server_location = args[3];

            StreamReader sr = new StreamReader(input_file);

            string line = sr.ReadLine();

            int total = 0;
            int faulty = 0;

            Console.Write("Reading TSV...");

            List<maledata> data = new List<maledata>();
            List<string> faulty_record = new List<string>();

            while ((line = sr.ReadLine()) != null)
            {
                read = line.Split(seperators, StringSplitOptions.None);

                if (read.Length <= 9)
                {
                    maledata read_data = new maledata();

                    read_data.account_id = read[0];
                    read_data.create_date = read[1];
                    read_data.profile = read[2];
                    read_data.liked_count = read[3];
                    read_data.log_predicted_liked_count = read[4];
                    read_data.log_liked_count = read[5];
                    read_data.account_id2 = read[6];
                    read_data.relative_path = read[7];
                    read_data.effect = read[8];

                    total++;

                    data.Add(read_data);
                }
                else
                {
                    faulty++;
                    faulty_record.Add(line);
                }
            }

            if (faulty == 0)
                Console.WriteLine($"read {total} records");
            else
            {
                Console.WriteLine($"read {total} records and {faulty} faulty records in faulty.tsv:");
                Console.WriteLine("");

                if (File.Exists("faulty.tsv"))
                    File.Delete("faulty.tsv");

                File.WriteAllLines("faulty.tsv", faulty_record);
            }

            List<string> outputlist = new List<string>();

            string outputdata = "\"account_id\"\t\"create_date\"\t\"profile\"\t\"liked_count\"\t\"IS_F.log_predicted_liked_count\"\t\"IS_F.log_liked_count\"\t\"accountId\"\t\"pictureRelativePath\"\t\"effect_imageAndPrediction\"\t\"phrases\"\t\"sentiment_score\"";

            outputlist.Add(outputdata);

            var phrases_client = new RestClient($"https://{server_location}.api.cognitive.microsoft.com/text/analytics/v2.0/keyPhrases");
            var sentiment_sclient = new RestClient($"https://{server_location}.api.cognitive.microsoft.com/text/analytics/v2.0/sentiment");

            for (int counter = 0; counter < data.Count; counter++)
            {
                Console.Write($"\rProcessing {counter} of {data.Count}");
                maledata eachdata = data[counter];

                if (eachdata.profile != "")
                {
                    string phrases = analyze_phrases(eachdata.profile, analytics_key, server_location, phrases_client);

                    double sentiment = analyze_sentiment(eachdata.profile, analytics_key, server_location, sentiment_sclient);

                    outputdata = $"{eachdata.account_id}\t{eachdata.create_date}\t{eachdata.profile}\t{eachdata.liked_count}\t{eachdata.log_predicted_liked_count}\t{eachdata.log_liked_count}\t{eachdata.account_id2}\t{eachdata.relative_path}\t{eachdata.effect}\t\"{phrases}\"\t\"{sentiment}\"";

                    outputlist.Add(outputdata);
                }
                else
                {
                    outputdata = $"{eachdata.account_id}\t{eachdata.create_date}\t{eachdata.profile}\t{eachdata.liked_count}\t{eachdata.log_predicted_liked_count}\t{eachdata.log_liked_count}\t{eachdata.account_id2}\t{eachdata.relative_path}\t{eachdata.effect}\t\"\"\t\"\"";

                    outputlist.Add(outputdata);
                }

                if (File.Exists(output_file))
                    File.Delete(output_file);

                File.WriteAllLines(output_file, outputlist);
            }

            Console.Write($"\rDone.                                      \r\n");
        }

        static string getJson(string input, string type, string key, string server_location, RestClient client)
        {
            var request = new RestRequest(Method.POST);

            input = input.Substring(1, input.Length - 2);

            request.AddHeader("cache-control", "no-cache");
            request.AddHeader("accept", "application/json");
            request.AddHeader("content-type", "application/json");
            request.AddHeader("ocp-apim-subscription-key", key);
            request.AddParameter("application/json", "{\r\n        \"documents\": [\r\n            {\r\n                \"language\": \"ja\",\r\n                \"id\": \"1\",\r\n                \"text\": \"" + input + "\"\r\n            }\r\n        ]\r\n    }", ParameterType.RequestBody);

            retry:

            IRestResponse response = client.Execute(request);

            string returned_json = response.Content;

            int waitseconds = 60;

            if (returned_json.Contains("Rate limit"))
            {
                string[] jsonsplits = returned_json.Split(':');

                foreach (string splitline in jsonsplits)
                {
                    if (splitline.Contains("second"))
                    {
                        string[] secondsplit = splitline.Split(' ');

                        for (int counter = 0; counter <= secondsplit.Length; counter++)
                        {
                            if (secondsplit[counter + 1].Contains("second"))
                            {
                                waitseconds = int.Parse(secondsplit[counter]);
                                goto skipcounting;
                            }
                        }
                    }
                }

                skipcounting:

                Console.WriteLine("");

                Console.WriteLine($"Asked to wait for {waitseconds} second(s)");

                for (int counter = waitseconds; counter >= 0; counter--)
                {
                    Console.Write($"\r{counter} seconds left ");
                    System.Threading.Thread.Sleep(1000);
                }

                Console.WriteLine("");
                goto retry;
            }

            if (returned_json.Contains("BadRequest"))
                return "error processing this profile";

            return returned_json;
        }

        static string analyze_phrases(string input, string key, string server_location, RestClient pclient)
        {
            if (input.Length > 0)
            {
                string phrases = "";

                string returned_json = getJson(input, "keyPhrases", key, server_location, pclient);

                if (returned_json != "error processing this profile")
                {
                    Newtonsoft.Json.Linq.JObject jObject = Newtonsoft.Json.Linq.JObject.Parse(returned_json);

                    foreach (string singlephrase in jObject["documents"][0]["keyPhrases"])
                        phrases += singlephrase + ",";

                    if (phrases != "")
                        phrases = phrases.Substring(0, phrases.Length - 1);

                    return phrases;
                }
                else
                    return "error processing this profile";
            }

            return "no input";
        }

        static double analyze_sentiment(string input, string key, string server_location, RestClient sclient)
        {
            if (input.Length > 0)
            {
                string returned_json = getJson(input, "sentiment", key, server_location, sclient);

                if (returned_json == "error processing this profile")
                    return 0;

                Newtonsoft.Json.Linq.JObject jObject = Newtonsoft.Json.Linq.JObject.Parse(returned_json);

                string sent = (string)jObject["documents"][0]["score"];

                float value = float.Parse(sent);

                return Math.Truncate(value * 100);
            }

            return 0;
        }
    }
}
