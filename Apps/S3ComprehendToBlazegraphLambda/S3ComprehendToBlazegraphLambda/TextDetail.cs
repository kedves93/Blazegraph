using Amazon.Comprehend.Model;
using System.Collections.Generic;

namespace S3ComprehendToBlazegraphLambda
{
    public class TextDetail
    {
        public IEnumerable<Entity> Entities { get; set; }
    }
}