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

        await ListBuckets(client);
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
            return response.HttpStatusCode == System.Net.HttpStatusCode.OK;
        }
        catch (AmazonS3Exception ex)
        {
            Console.WriteLine($"Error creating bucket: '{ex.Message}'");
            return false;
        }
    }

    public static async Task<bool> DeleteBucketAsync(IAmazonS3 client, string bucketName)
    {
        var result = false;
        var request = new DeleteBucketRequest {
            BucketName = bucketName
        };

        try {
            var response = await client.DeleteBucketAsync(request);
            // Notice the standard HTTP REST response code on an operation like 'DELETE'
            result = (response.HttpStatusCode == System.Net.HttpStatusCode.NoContent);
            if (! result)
                Console.WriteLine($"Error deleting bucket: '{response.HttpStatusCode}\n");

        }  
        catch (AmazonS3Exception ex) {
            Console.WriteLine($"Error deleting bucket: '{ex.Message}'");
        }
        return result;
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


}
