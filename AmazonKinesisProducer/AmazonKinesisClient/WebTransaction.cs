using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;


namespace AmazonKinesisConsumer
{
	public class WebTransaction
	{
		private string[] _validMethods = { "get", "post", "put", "delete", "head", "options", "trace", "connect" };
		private int _minResponseTimeMs = 0;
		private int _maxResponseTimeMs = 30000;

		public long UtcDateUnixMs { get; set; }
		public string CustomerName { get; set; }
		public string Url { get; set; }
		public string WebMethod { get; set; }
		public int ResponseTimeMs { get; set; }

		public DateTime ObservationDateUtc
		{
			get
			{
				return UnixTimeStampToDateTime(UtcDateUnixMs);
			}
		}

		public List<string> Validate()
		{
			List<string> brokenRules = new List<string>();
			if (!IsWebMethodValid())
			{
				brokenRules.Add(string.Format("Invalid web method: {0}", WebMethod));
			}
			if (!IsResponseTimeValid())
			{
				brokenRules.Add(string.Format("Response time outside acceptable limits: {0}", ResponseTimeMs));
			}
			if (!IsValidUrl())
			{
				brokenRules.Add(string.Format("Invalid URL: {0}", Url));
			}
			return brokenRules;
		}			

		public string FormattedObservationDateMinutes()
		{
			String dateFormat = "yyyy-MM-dd-HH-mm";			
			return ObservationDateUtc.ToString(dateFormat);
		}
				
		public string ToTabDelimitedString()
		{
			StringBuilder sb = new StringBuilder();
			string delimiter = "\t";
			sb.Append(CustomerName)
				.Append(delimiter)
				.Append(Url)
				.Append(delimiter)
				.Append(WebMethod)
				.Append(delimiter)
				.Append(ResponseTimeMs)
				.Append(delimiter)
				.Append(UtcDateUnixMs)
				.Append(delimiter)
				.Append(ObservationDateUtc);
			return sb.ToString();
		}

		private bool IsWebMethodValid()
		{
			return _validMethods.Contains(WebMethod.ToLower());
		}

		private bool IsResponseTimeValid()
		{
			if (ResponseTimeMs < _minResponseTimeMs
				|| ResponseTimeMs > _maxResponseTimeMs)
			{
				return false;
			}

			return true;
		}

		private bool IsValidUrl()
		{
			Uri uri;
			string urlToValidate = Url;
			if (!urlToValidate.Contains(Uri.SchemeDelimiter)) urlToValidate = string.Concat(Uri.UriSchemeHttp, Uri.SchemeDelimiter, urlToValidate);
			if (Uri.TryCreate(urlToValidate, UriKind.RelativeOrAbsolute, out uri))
			{
				try
				{
					if (Dns.GetHostAddresses(uri.DnsSafeHost).Length > 0)
					{
						return true;
					}
				}
				catch
				{
					return false;
				}
			}

			return false;
		}	

		private DateTime UnixTimeStampToDateTime(long unixTimeMillis)
		{
			DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
			DateTime converted = epoch.AddMilliseconds(unixTimeMillis);
			return converted;
		}

	}
}
