using Amazon.Comprehend;
using Amazon.Comprehend.Model;
using System.Collections.Generic;

namespace S3ToComprehendLambda
{
    public class TextDetail
    {
        public string Id { get; set; }

        public IEnumerable<Entity> Entities { get; set; }

        public SentimentType Sentiment { get; set; }
    }
}