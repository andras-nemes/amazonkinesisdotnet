using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

namespace AmazonKinesisConsumer
{
	public class AmazonS3DataStorage : IRawDataStorage
	{
		private readonly string _topBucketName;

		public AmazonS3DataStorage(String topBucketName)
		{
			if (String.IsNullOrEmpty(topBucketName)) throw new ArgumentNullException("S3 bucket name cannot be empty!");
			_topBucketName = topBucketName;
		}

		public void Save(IEnumerable<WebTransaction> webTransactions)
		{
			Dictionary<string, List<WebTransaction>> groups = GroupRecordsPerCustomerAndDate(webTransactions);
			foreach (var kvp in groups)
			{
				try
				{
					if (groups.Values.Any())
					{
						SaveInS3(kvp.Key, kvp.Value);
					}
				}
				catch (Exception ex)
				{
					Console.WriteLine(string.Concat("Failed to write web transactions in ", kvp.Key, " to the S3 bucket"));
				}
			}
		}

		private void SaveInS3(String fileKey, List<WebTransaction> webTransactions)
		{
			if (!fileKey.EndsWith(".txt"))
			{
				fileKey += ".txt";
			}
			WebTransaction first = webTransactions.First();
			string containingFolder = BuildContainingFolderName(first);
			//check if folder exists
			using (IAmazonS3 s3Client = GetAmazonS3Client())
			{
				try
				{
					ListObjectsRequest findFolderRequest = new ListObjectsRequest();
					findFolderRequest.BucketName = _topBucketName;
					findFolderRequest.Delimiter = "/";
					findFolderRequest.Prefix = containingFolder;
					ListObjectsResponse findFolderResponse = s3Client.ListObjects(findFolderRequest);
					List<string> commonPrefixes = findFolderResponse.CommonPrefixes;
					if (commonPrefixes.Any())
					{
						SaveWebTransactionsInFolder(containingFolder, fileKey, webTransactions);
					}
					else //need to create S3 bucket first
					{
						PutObjectRequest folderRequest = new PutObjectRequest();
						folderRequest.BucketName = _topBucketName;
						string delimiter = "/";
						String folderKey = string.Concat(containingFolder, delimiter);
						folderRequest.Key = folderKey;
						folderRequest.InputStream = new MemoryStream(new byte[0]);
						PutObjectResponse folderResponse = s3Client.PutObject(folderRequest);
						SaveWebTransactionsInFolder(containingFolder, fileKey, webTransactions);
					}
				}
				catch (AmazonS3Exception e)
				{
					Console.WriteLine("Folder existence check or folder creation has failed.");
					Console.WriteLine("Amazon error code: {0}",
						string.IsNullOrEmpty(e.ErrorCode) ? "None" : e.ErrorCode);
					Console.WriteLine("Exception message: {0}", e.Message);
				}
			}
		}

		private void SaveWebTransactionsInFolder(string folderName, string fileKey, List<WebTransaction> webTransactionsInFile)
		{
			string fileContents = BuildRawDataFileContent(webTransactionsInFile);
			using (IAmazonS3 s3Client = GetAmazonS3Client())
			{
				try
				{
					PutObjectRequest putObjectRequest = new PutObjectRequest();
					putObjectRequest.ContentBody = fileContents;
					String delimiter = "/";
					putObjectRequest.BucketName = string.Concat(_topBucketName, delimiter, folderName);
					putObjectRequest.Key = fileKey;
					PutObjectResponse putObjectResponse = s3Client.PutObject(putObjectRequest);
				}
				catch (AmazonS3Exception e)
				{
					Console.WriteLine("Failed to save the raw data observations in S3.");
					Console.WriteLine("Amazon error code: {0}",
						string.IsNullOrEmpty(e.ErrorCode) ? "None" : e.ErrorCode);
					Console.WriteLine("Exception message: {0}", e.Message);
				}
			}
		}

		private String BuildRawDataFileContent(List<WebTransaction> webTransactions)
		{
			StringBuilder recordBuilder = new StringBuilder();
			int size = webTransactions.Count;
			for (int i = 0; i < size; i++)
			{
				recordBuilder.Append(webTransactions[i].ToTabDelimitedString());
				if (i < size - 1)
				{
					recordBuilder.Append(Environment.NewLine);
				}
			}
			return recordBuilder.ToString();
		}

		private string BuildContainingFolderName(WebTransaction webTransaction)
		{
			DateTime observationDate = webTransaction.ObservationDateUtc;
			int year = observationDate.Year;
			string monthString = FormatDateUnitWithLeadingZeroes(observationDate.Month);
			string dayString = FormatDateUnitWithLeadingZeroes(observationDate.Day);
			string hourString = FormatDateUnitWithLeadingZeroes(observationDate.Hour);
			int minuteInterval = GetMinuteInterval(observationDate.Minute);
			string minuteIntervalString = FormatDateUnitWithLeadingZeroes(minuteInterval);
			string folderNameDelimiter = "-";
			return string.Concat(minuteIntervalString, folderNameDelimiter, hourString, folderNameDelimiter
				, dayString, folderNameDelimiter, monthString, folderNameDelimiter, year);
		}

		private string FormatDateUnitWithLeadingZeroes(int dateUnit)
		{
			String formatted = dateUnit < 10 ? string.Concat("0", dateUnit) : dateUnit.ToString();
			return formatted;
		}

		private Dictionary<string, List<WebTransaction>> GroupRecordsPerCustomerAndDate(IEnumerable<WebTransaction> allWebTransactions)
		{
			Dictionary<string, List<WebTransaction>> group = new Dictionary<string, List<WebTransaction>>();
			foreach (WebTransaction wt in allWebTransactions)
			{
				String key = string.Concat(wt.CustomerName, "-", wt.FormattedObservationDateMinutes());
				if (group.ContainsKey(key))
				{
					List<WebTransaction> transactionsInGroup = group[key];
					transactionsInGroup.Add(wt);
				}
				else
				{
					List<WebTransaction> transactionsInGroup = new List<WebTransaction>();
					transactionsInGroup.Add(wt);
					group[key] = transactionsInGroup;
				}
			}

			return group;
		}

		private IAmazonS3 GetAmazonS3Client()
		{
			return Amazon.AWSClientFactory.CreateAmazonS3Client(RegionEndpoint.EUWest1);
		}

		private int GetMinuteInterval(int minute)
		{
			int res = 0;
			if (minute > 29)
			{
				res = 30;
			}
			return res;
		}
	}
}
