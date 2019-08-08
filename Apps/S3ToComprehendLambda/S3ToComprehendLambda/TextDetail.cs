using Amazon.Comprehend;
using Amazon.Comprehend.Model;
using System.Collections.Generic;

namespace S3ToComprehendLambda
{
    public class TextDetail
    {
        public IEnumerable<Entity> Entities { get; set; }

        public SentimentType Sentiment { get; set; }
    }
}