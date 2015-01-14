using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Newtonsoft.Json;

namespace AmazonKinesisProducer
{
	class Program
	{
		static void Main(string[] args)
		{			
			List<WebTransaction> webTransactions = GetTransactions();
			SendWebTransactionsToQueue(webTransactions);

			Console.WriteLine("Main done...");
			Console.ReadKey();
		}

		private static void SendWebTransactionsToQueue(List<WebTransaction> transactions)
		{
			AmazonKinesisConfig config = new AmazonKinesisConfig();
			config.RegionEndpoint = Amazon.RegionEndpoint.EUWest1;			
			AmazonKinesisClient kinesisClient = new AmazonKinesisClient(config);
			String kinesisStreamName = ConfigurationManager.AppSettings["KinesisStreamName"];

			foreach (WebTransaction wt in transactions)
			{
				string dataAsJson = JsonConvert.SerializeObject(wt);
				byte[] dataAsBytes = Encoding.UTF8.GetBytes(dataAsJson);
				using (MemoryStream memoryStream = new MemoryStream(dataAsBytes))
				{
					try
					{						
						PutRecordRequest requestRecord = new PutRecordRequest();
						requestRecord.StreamName = kinesisStreamName;
						requestRecord.PartitionKey = "url-response-times";
						requestRecord.Data = memoryStream;

						PutRecordResponse responseRecord = kinesisClient.PutRecord(requestRecord);
						Console.WriteLine("Successfully sent record {0} to Kinesis. Sequence number: {1}", wt.Url, responseRecord.SequenceNumber);
					}
					catch (Exception ex)
					{
						Console.WriteLine("Failed to send record {0} to Kinesis. Exception: {1}", wt.Url, ex.Message);
					}
				}
			}
		}

		private static long ConvertToUnixMillis(DateTime dateToConvert)
		{
			return Convert.ToInt64(dateToConvert.Subtract(new DateTime(1970, 1, 1, 0, 0, 0, 0)).TotalMilliseconds);
		}

		private static List<WebTransaction> GetTransactions()
		{
			List<WebTransaction> webTransactions = new List<WebTransaction>();
			Console.WriteLine("Enter your web transactions. ");
			Console.Write("URL - type 'x' and press Enter to exit: ");
			string url = Console.ReadLine();
			while (url != "x")
			{
				WebTransaction wt = new WebTransaction();
				wt.Url = url;
				wt.UtcDateUnixMs = ConvertToUnixMillis(DateTime.UtcNow);

				Console.Write("Customer name: ");
				string customerName = Console.ReadLine();
				wt.CustomerName = customerName;

				Console.Write("Response time (ms): ");
				int responseTime = Convert.ToInt32(Console.ReadLine());
				wt.ResponseTimeMs = responseTime;

				Console.Write("Web method: ");
				string method = Console.ReadLine();
				wt.WebMethod = method;

				webTransactions.Add(wt);

				Console.Write("URL - enter 'x' and press enter to exit: ");
				url = Console.ReadLine();
			}
			return webTransactions;
		}
	}
}
