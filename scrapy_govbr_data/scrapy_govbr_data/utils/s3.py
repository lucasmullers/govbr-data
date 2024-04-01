import boto3

class S3():
    
    def __init__(self, profile_name: str = "dbt", resource: str = "s3"):
        """
        Initializes a new instance of the class.

        Args:
            profile_name (str, optional): The name of the AWS profile to use. Defaults to "dbt".
            resource (str, optional): The name of the AWS resource to use. Defaults to "s3".

        Returns:
            None
        """
        self.profile_name = profile_name
        self.resource = resource

        self.session = boto3.Session(profile_name=self.profile_name)
        self.resource = self.session.resource(self.resource)
    
    def upload_file(self, file_path: str, bucket_name: str, object_name: str):
        """
        Uploads a file to an S3 bucket.
        Args:
            file_path (str): The path to the file to be uploaded.
            bucket_name (str): The name of the S3 bucket.
            object_name (str): The name of the object in the S3 bucket.
        Returns:
            None
        """
        self.resource.Bucket(bucket_name).upload_file(file_path, object_name)
    
    def list_objects_by_prefix(self, bucket_name: str, prefix: str):
        return self.resource.Bucket(bucket_name).objects.filter(Prefix=prefix)