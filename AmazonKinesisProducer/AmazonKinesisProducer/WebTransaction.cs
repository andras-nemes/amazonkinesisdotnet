using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AmazonKinesisProducer
{
	public class WebTransaction
	{
		public long UtcDateUnixMs { get; set; }
		public string CustomerName { get; set; }
		public string Url { get; set; }
		public string WebMethod { get; set; }
		public int ResponseTimeMs { get; set; }
	}
}
