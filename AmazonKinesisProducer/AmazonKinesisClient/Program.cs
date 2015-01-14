using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AmazonKinesisConsumer
{
	class Program
	{
		static void Main(string[] args)
		{
			ReadFromStream();

			Console.WriteLine("Main done...");
			Console.ReadKey();
		}

		private static void ReadFromStream()
		{
			IRawDataStorage rawDataStorage = new AmazonS3DataStorage("raw-urls-data"); //new FileBasedDataStorage(@"c:\raw-data\storage.txt");
			AmazonKinesisConfig config = new AmazonKinesisConfig();
			config.RegionEndpoint = Amazon.RegionEndpoint.EUWest1;
			AmazonKinesisClient kinesisClient = new AmazonKinesisClient(config);
			String kinesisStreamName = ConfigurationManager.AppSettings["KinesisStreamName"];

			DescribeStreamRequest describeRequest = new DescribeStreamRequest();
			describeRequest.StreamName = kinesisStreamName;

			DescribeStreamResponse describeResponse = kinesisClient.DescribeStream(describeRequest);
			List<Shard> shards = describeResponse.StreamDescription.Shards;

			foreach (Shard shard in shards)
			{
				GetShardIteratorRequest iteratorRequest = new GetShardIteratorRequest();
				iteratorRequest.StreamName = kinesisStreamName;
				iteratorRequest.ShardId = shard.ShardId;
				iteratorRequest.ShardIteratorType = ShardIteratorType.TRIM_HORIZON;

				GetShardIteratorResponse iteratorResponse = kinesisClient.GetShardIterator(iteratorRequest);
				string iteratorId = iteratorResponse.ShardIterator;

				while (!string.IsNullOrEmpty(iteratorId))
				{
					GetRecordsRequest getRequest = new GetRecordsRequest();
					getRequest.Limit = 1000;
					getRequest.ShardIterator = iteratorId;

					GetRecordsResponse getResponse = kinesisClient.GetRecords(getRequest);
					string nextIterator = getResponse.NextShardIterator;
					List<Record> records = getResponse.Records;

					if (records.Count > 0)
					{
						Console.WriteLine("Received {0} records. ", records.Count);
						List<WebTransaction> newWebTransactions = new List<WebTransaction>();
						foreach (Record record in records)
						{
							string json = Encoding.UTF8.GetString(record.Data.ToArray());
							try
							{
								JToken token = JContainer.Parse(json);
								try
								{									
									WebTransaction wt = JsonConvert.DeserializeObject<WebTransaction>(json);
									List<string> validationErrors = wt.Validate();
									if (!validationErrors.Any())
									{
										Console.WriteLine("Valid entity: {0}", json);
										newWebTransactions.Add(wt);
									}
									else
									{
										StringBuilder exceptionBuilder = new StringBuilder();
										exceptionBuilder.Append("Invalid WebTransaction object from JSON: ")
											.Append(Environment.NewLine).Append(json)
											.Append(Environment.NewLine).Append("Validation errors: ")
											.Append(Environment.NewLine);
										foreach (string error in validationErrors)
										{
											exceptionBuilder.Append(error).Append(Environment.NewLine);																										
										}
										Console.WriteLine(exceptionBuilder.ToString());
									}									
								}
								catch (Exception ex)
								{
									//simulate logging
									Console.WriteLine("Could not parse the following message to a WebTransaction object: {0}", json);
								}
							}
							catch (Exception ex)
							{
								//simulate logging
								Console.WriteLine("Could not parse the following message, invalid json: {0}", json);
							}
						}

						if (newWebTransactions.Any())
						{
							try
							{
								rawDataStorage.Save(newWebTransactions);
								Console.WriteLine("Saved all new web transactions to the data store.");
							}
							catch (Exception ex)
							{
								Console.WriteLine("Failed to save the web transactions to file: {0}", ex.Message);
							}
						}
					}

					iteratorId = nextIterator;
				}
			}
		}
	}
}
