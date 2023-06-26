import boto3
import json

class Secrets:
    """
    Generic class to get secrets from aws secret manager.
  
    Attributes:
        SECRET_NAME: Name of the secret
        REGION_NAME: Region of the Secret manager
    """

    def __init__(self, secret_name, region_name):
        self.get_secretDict(secret_name, region_name)
        
    #Getting the secret dictionary from secret manager
    def get_secretDict(self, secret_name, region_name):

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        
  

        self.secretDict = json.loads(get_secret_value_response['SecretString'])
    'Getting individual secret key value pair'
    def get_secret_value(self, secret_key):
        return self.secretDict[secret_key]
 

# For testing locally
if __name__ == "__main__":
           new_test = Secrets('/prod/db/snowflake','us-west-2' )
           print(new_test.get_secret_value('SNOW_ACCOUNT'))
     