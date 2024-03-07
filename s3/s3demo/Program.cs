using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Amazon.S3.Util;

// See https://aka.ms/new-console-template for more information

public class S3Demo
{
    public static async Task Main()
    {
        Console.WriteLine("S3 .NET Client Demo");

        // Initialize the S3 client with the 'app-user' credentials read from the ~/.aws/config file
        Amazon.Profile profile = new Amazon.Profile("app-user");
        AmazonS3Config s3config = new AmazonS3Config
        {
            Profile = new Amazon.Profile("app-user")
        };
        IAmazonS3 client = new AmazonS3Client(s3config);

        var bucketName = "mark-test-9702144567";
        if (await BucketExists(client, bucketName))
        {
            Console.WriteLine($"Bucket {bucketName} already exists.\n");
        }
        else
        {
            if (await CreateBucketAsync(client, bucketName))
                Console.WriteLine($"Successfully created bucket: {bucketName}.\n");
            else
                Console.WriteLine($"Could not create bucket: {bucketName}.\n");

        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        await ListBuckets(client);

        // Upload a file
        var sourceFile = "notes.csv";
        var metadata = new Dictionary<string, string>
                                   {
                                       { "myVal", "lab2-testing-upload" }
                                   };
        await UploadObjectAsync(client, bucketName, sourceFile, "text/csv", metadata);

        // List bucket Contents
        await ListBucketContents(client, bucketName);

        var url = GeneratePresignedURL(client, bucketName, sourceFile, 3600);
        Console.WriteLine($"Presigned URL = {url}");

        if (await DeleteBucketAsync(client, bucketName))
            Console.WriteLine($"Successfully deleted bucket: {bucketName}.\n");
        else
            Console.WriteLine($"Could not delete bucket: {bucketName}.\n");
    }

    /// <summary>
    /// This is an example leveraging the code sample provided in the develper guide
    /// https://docs.aws.amazon.com/sdk-for-net/v3/developer-guide/csharp_s3_code_examples.html
    /// </summary>
    ///
    /// <param name="client">An initialized S3 Client</param>
    /// <param name="bucketName">The name of the bucket to create</param>
    /// <returns>True if creation succeeds. False otherwise</returns> 
    public static async Task<bool> CreateBucketAsync(IAmazonS3 client, string bucketName)
    {
        try
        {
            var request = new PutBucketRequest
            {
                BucketName = bucketName,
            };

            var response = await client.PutBucketAsync(request);
            if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                await WaitForBucketCreationAsync(client, bucketName);
                return true;
            }
            else
            {
                Console.WriteLine($"Bucket creation did not return correct status code: '{response.HttpStatusCode}");
                return false;

            }
        }
        catch (AmazonS3Exception ex)
        {
            Console.WriteLine($"Error creating bucket: '{ex.Message}'");
            return false;
        }
    }

    public static async Task<bool> ListBucketContents(IAmazonS3 client, string bucketName)
    {
        try
        {
            var request = new ListObjectsV2Request
            {
                BucketName = bucketName,
                MaxKeys = 5,
            };

            Console.WriteLine("--------------------------------------");
            Console.WriteLine($"Listing the contents of {bucketName}:");
            Console.WriteLine("--------------------------------------");

            ListObjectsV2Response response;

            do
            {
                response = await client.ListObjectsV2Async(request);

                response.S3Objects
                    .ForEach(obj => Console.WriteLine($"{obj.Key,-35}{obj.LastModified.ToShortDateString(),10}{obj.Size,10}"));

                // If the response is truncated, set the request ContinuationToken
                // from the NextContinuationToken property of the response.
                request.ContinuationToken = response.NextContinuationToken;
            }
            while (response.IsTruncated);

            return true;
        }
        catch (AmazonS3Exception ex)
        {
            Console.WriteLine($"Error encountered on server. Message:'{ex.Message}' getting list of objects.");
            return false;
        }
    }

    public static async Task<bool> DeleteBucketAsync(IAmazonS3 client, string bucketName)
    {
        var result = false;

        // First, clean bucket contents
        await ClearBucketContents(client, bucketName);

        // Now, delete the bucket itself
        var request = new DeleteBucketRequest
        {
            BucketName = bucketName
        };

        try
        {
            var response = await client.DeleteBucketAsync(request);
            // Notice the standard HTTP REST response code on an operation like 'DELETE'
            result = (response.HttpStatusCode == System.Net.HttpStatusCode.NoContent);
            if (!result)
                Console.WriteLine($"Error deleting bucket: '{response.HttpStatusCode}\n");

        }
        catch (AmazonS3Exception ex)
        {
            Console.WriteLine($"Error deleting bucket: '{ex.Message}'");
        }
        return result;
    }

    public static async Task<bool> ClearBucketContents(IAmazonS3 client, string bucketName)
    {
        // Iterate over the contents of the bucket and delete all objects.
        var request = new ListObjectsV2Request
        {
            BucketName = bucketName,
        };

        try
        {
            ListObjectsV2Response response;

            do
            {
                response = await client.ListObjectsV2Async(request);
                response.S3Objects
                    .ForEach(async obj => await client.DeleteObjectAsync(bucketName, obj.Key));

                // If the response is truncated, set the request ContinuationToken
                // from the NextContinuationToken property of the response.
                request.ContinuationToken = response.NextContinuationToken;
            }
            while (response.IsTruncated);

            return true;
        }
        catch (AmazonS3Exception ex)
        {
            Console.WriteLine($"Error deleting objects: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// Lists the buckets across all regions for the given account credentials
    /// </summary>
    /// 
    /// <param name="client">An initialized S3 Client</param>
    /// 
    public static async Task ListBuckets(IAmazonS3 client)
    {
        // Issue call
        ListBucketsResponse response = await client.ListBucketsAsync();

        // View response data
        Console.WriteLine("Buckets owner - {0}", response.Owner.DisplayName);
        foreach (S3Bucket bucket in response.Buckets)
        {
            Console.WriteLine("Bucket {0}, Created on {1}", bucket.BucketName, bucket.CreationDate);
        }
    }

    /// <summary>
    /// Uses the AmazonS3Util version as the version on the S3Client has been deprecated
    /// </summary>
    /// <param name="client">An initialized S3 Client</param>
    /// <param name="bucketName">The name of the bucket to create</param>
    /// <returns>True if the bucket exists. False otherwise</returns> 
    public static async Task<bool> BucketExists(IAmazonS3 client, string bucketName)
    {
        return await AmazonS3Util.DoesS3BucketExistV2Async(client, bucketName);
        // Deprecated - Don't use this any more!!
        //return await client.DoesS3BucketExistAsync(bucketName);
    }

    /// <summary>
    /// Will wait in this method until bucket exists and will re-assess every 1/2 second
    /// </summary>
    /// <param name="client">An initialized S3 Client</param>
    /// <param name="bucketName">The name of the bucket to monitor</param>
    /// <returns></returns>
    public static async Task WaitForBucketCreationAsync(IAmazonS3 client, string bucketName)
    {
        bool bucketExists = await AmazonS3Util.DoesS3BucketExistV2Async(client, bucketName);
        while (!bucketExists)
        {
            System.Threading.Thread.Sleep(500);
            bucketExists = await AmazonS3Util.DoesS3BucketExistV2Async(client, bucketName);
        }

    }

    /// <summary>
    /// Uploads he specified local file to S3 in the specified bucket using the same key as the file name
    /// </summary>
    /// <param name="client"></param>
    /// <param name="bucketName"></param>
    /// <param name="sourceFileName"></param>
    /// <param name="contentType"></param>
    /// <param name="metadata"></param>
    /// <returns></returns> 
    /// 
    public static async Task<bool> UploadObjectAsync(IAmazonS3 client, string bucketName, string sourceFileName, string contentType, IDictionary<string, string> metadata)
    {
        var request = new PutObjectRequest
        {
            BucketName = bucketName,
            Key = sourceFileName,
            FilePath = System.IO.Path.Join(Environment.CurrentDirectory, sourceFileName),
            ContentType = contentType
        };

        foreach (var key in metadata.Keys)
        {
            request.Metadata.Add(key, metadata[key]);
        }

        try
        {
            await client.PutObjectAsync(request);
            return true;
        }
        catch (AmazonS3Exception ex)
        {
            Console.WriteLine($"Error uploading file: '{ex.Message}'");
            return false;
        }

    }

    /// <summary>
    /// Generate a presigned URL that can be used to access the file named
    /// in the objectKey parameter for the amount of time specified in the
    /// duration parameter.
    /// </summary>
    /// <param name="client">An initialized S3 client object used to call
    /// the GetPresignedUrl method.</param>
    /// <param name="bucketName">The name of the S3 bucket containing the
    /// object for which to create the presigned URL.</param>
    /// <param name="objectKey">The name of the object to access with the
    /// presigned URL.</param>
    /// <param name="duration">The length of time for which the presigned
    /// URL will be valid.</param>
    /// <returns>A string representing the generated presigned URL.</returns>
    public static string GeneratePresignedURL(IAmazonS3 client, string bucketName, string objectKey, double duration)
    {
        string urlString = string.Empty;
        try
        {
            var request = new GetPreSignedUrlRequest()
            {
                BucketName = bucketName,
                Key = objectKey,
                Expires = DateTime.UtcNow.AddHours(duration),
            };
            urlString = client.GetPreSignedURL(request);
        }
        catch (AmazonS3Exception ex)
        {
            Console.WriteLine($"Error:'{ex.Message}'");
        }

        return urlString;
    }

    /// <summary>
    /// Simplistic 'get file' operation that returns the file from S3 as a String of data 
    /// </summary>
    /// <param name="client"></param>
    /// <param name="bucketName"></param>
    /// <param name="name"></param>
    /// <returns></returns>
    public static async Task<string> GetFileAsync(IAmazonS3 client, string bucketName, string name)
    {
        string contents = null;

        var request = new GetObjectRequest
        {
            BucketName = bucketName,
            Key = name
        };

        var response = await client.GetObjectAsync(request);
        using (var reader = new StreamReader(response.ResponseStream))
        {
            contents = reader.ReadToEnd();
        }

        return contents;

    }

}
