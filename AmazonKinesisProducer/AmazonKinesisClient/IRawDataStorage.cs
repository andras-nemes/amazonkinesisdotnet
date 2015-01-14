using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AmazonKinesisConsumer
{
	public interface IRawDataStorage
	{
		void Save(IEnumerable<WebTransaction> webTransactions);
	}
}
