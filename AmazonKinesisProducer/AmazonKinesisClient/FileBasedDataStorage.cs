using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AmazonKinesisConsumer
{
	public class FileBasedDataStorage : IRawDataStorage
	{
		private readonly FileInfo _fileName;

		public FileBasedDataStorage(string fileFullPath)
		{
			if (string.IsNullOrEmpty(fileFullPath)) throw new ArgumentNullException("File full path");
			_fileName = new FileInfo(fileFullPath);
			if (!_fileName.Exists)
			{
				throw new ArgumentException(string.Concat("Provided file path ", fileFullPath, " does not exist."));
			}			
		}
		
		public void Save(IEnumerable<WebTransaction> webTransactions)
		{
			StringBuilder stringBuilder = new StringBuilder();
			foreach (WebTransaction wt in webTransactions)
			{
				stringBuilder.Append(wt.ToTabDelimitedString()).Append(Environment.NewLine);
			}

			using (StreamWriter sw = File.AppendText(_fileName.FullName))
			{
				sw.Write(stringBuilder.ToString());
			}
		}
	}
}
