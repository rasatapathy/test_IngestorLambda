import json
import requests
import os
import urllib
import boto3
import bisect
import datetime
import functools   

class Article:
    def __init__(self, obj):

        # obj is the input from which data would be constructed
        self.obj = obj['data']

        # data is the new required object
        self.data = {
            # add all details common to every article
            'id': self.obj.get('id', ''),
            'title': self.obj.get('title', {}).get('englishConsumer', ''),
            'version': self.obj.get('version', ''),
            'type': self.obj.get('type', ''),
            'docType': self.obj.get('docType', ''),
        }
        if self.is_video():
            self.update_video_details()
        self.update_article_details()\
        .update_all_topics()\
        .update_extras()
    
    def is_video(self):
        return 'sources' in self.obj

    def update_video_details(self):
        self.data['video'] = {
            'thumbnail': self.obj.get('thumbnailImage', ''),
            'sources': self.obj.get('sources', ''),
            'closedCaptions': self.obj.get('closedCaptions', {}),
        }
        return self

    def update_article_details(self):
        self.data['article'] = {
            'title': self.obj.get('title', {}).get('englishConsumer', ''),
            'abstract': self.obj.get('abstract', {}).get('consumer', ''),
        }
        return self

    def update_all_topics(self):
        if 'article' not in self.data:
            self.update_article_details()
            
        topics = self.obj.get('topics', [])
        if self.is_video():
            self.update_all_topics_video()
        else:
            self.update_all_topics_article()
        return self

    def update_all_topics_video(self):
        if self.is_video():
            if 'article' not in self.data:
                self.update_article_details()
            topics = self.obj.get('topics', [])
            self.data['article']['topics'] = [
                    {
                        'id': topic.get('id', ''),
                        'type': topic.get('type', ''),
                        'infoItems': [
                            {
                                'header': 'Abstract',
                                'content': topic.get('abstract', {}).get('consumer', ''),
                            },
                            {
                                'header': 'Learning Objective',
                                'content': topic.get('learningObjectiveText', {}).get('html', ''),

                            },
                            {
                                'header': 'Teaser',
                                'content': topic.get('teaser', {}).get('html', ''),
                            },
                        ]
                    }
                for topic in topics]

    def update_all_topics_article(self):
        if not self.is_video():
            if 'article' not in self.data:
                self.update_article_details()
            topics = self.obj.get('topics', [])
            self.data['article']['extras'] = [
                {
                    'html': topic.get('html', ''),
                }
            for topic in topics]

    def update_extras(self):
        self.data['extras'] = []
        self.update_transcript()\
            .update_legal_info()\
            .update_credits()
        return self
    
    def update_transcript(self):
        if 'extras' not in self.data:
            self.update_extras()
        self.data['extras'].append(
            {
                "header": "Transcript",
			    "content": self.obj.get('transcript', {}).get('html', ''),
            }
        )
        return self
    
    def update_legal_info(self):
        if 'extras' not in self.data:
            self.update_extras()
        legal = self.obj.get('legal', {})
        self.data['extras'].append(
            {
                "header": "Legal",
			    "content": [
                    {
					    "content": legal.get('logo', ''),
				    },
				    {
					    "content": legal.get('copyright', ''),
				    },
                    {
					    "header": "disclaimer",
					    "content": legal.get('disclaimer', ''),
				    },
				    {
					    "header": "Terms of use",
					    "content": legal.get('termsOfUseUrl', ''),
				    },
				    {
					    "header": "Privacy policy",
					    "content": legal.get('privacyPolicyUrl',''),
				    },
				    {
					    "header": "More information",
					    "content": legal.get('moreInformationUrl',''),
				    },
                    {
					    "content": legal.get('toLearnMoreHtml', ''),
				    },
				    {
					    "content": legal.get('yourUseOfThisInformationHtml', '')
				    }
                ]

            }
        )
        return self

    def update_credits(self):
        if 'extras' not in self.data:
            self.update_extras()

        credits = self.obj.get('credits', {})
        self.data['extras'].append(
            {
                "header": "Credits",
                "content": [
                    {
                        'header': 'Author',
                        'content': credits.get('author', {}).get('name', ''),
                    },
                    {
                        'header': 'Primary Reviewers',
                        'content': [
                            primary_reviewer.get('name', '')
                        for primary_reviewer in credits.get('primaryReviewers', [])],
                    },
                    {
                        'content': credits.get('html', ''),
                    },
                ]
            }
        )
        return self

    def update_related_topics(self):
        if 'extras' not in self.data:
            self.update_extras()
        related_topics = self.obj.get('relatedTopics', [])
        self.data['extras']['relatedTopics'] = [
            {
                'id': related_topic.get('id', ''),
                'title': related_topic.get('title', '')
            }
            for related_topic in related_topics]
        return self

    def update_all_old(self):
        # self.update_video_details()\
        #     .update_article_details()\
        #     .update_extras()\
        #     .update_all_topics()\
        #     .update_legal_info()\
        #     .update_credits()\
        #     .update_related_topics()
        return self

environ = {
    'AIH_HEALTHWISE_INVENTORY_URL': 'https://content.healthwise.net/inventory',
    'AWS_BUCKET_NAME': 'ambient-save-healthwise-articles-v1',
    'AWS_BUCKET_NAME_LOGS': 'ambient-ingestor-lambda-logs',
    'access_token_url': 'https://auth.healthwise.net/oauth2/token',
    'basic_auth': 'Basic YzQ1ZWEyMDQ2YmMwNGRkOGE4MTVhZTM0OTEzYzEzYTI6NjJZQmd2ZktvVXl6SVVmRjIyZytGdz09',
}

def get_bearer_token():
    payload='grant_type=client_credentials&scope=*'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': environ['basic_auth']
    }
    url = environ['access_token_url']
    response = requests.request("POST", url, headers=headers, data=payload)
    access_token=json.loads(response.text)['access_token']
    token = "Bearer "+ access_token
    return token

bearer_token = get_bearer_token()


AWS_BUCKET_NAME = environ['AWS_BUCKET_NAME']
AWS_BUCKET_NAME_LOGS = environ['AWS_BUCKET_NAME_LOGS']

def get_article_state(article_id, auth_version, authoring_version, failed_ids):
    # This demands binary search
    # Authoring Version should always be stored in Sorted format with key as article_id
    
    # checking if it is a failedID
    
    data = failed_ids.get('Body', [])
    left, right = 0, len(data) - 1
 
 
    while left <= right:
        mid = (left + right) // 2
        failed_id = data[mid]
        failed_article_id = failed_id.data.get('article_id', '')
        if failed_article_id == article_id:
            return "FAILED"
        elif failed_article_id < article_id:
            left = mid + 1
        else:
            right = mid - 1


    data = authoring_version.get('Body', [])
    left, right = 0, len(data) - 1
    while left <= right:
        mid = (left + right) // 2
        auth = data[mid]
        auth_article_id = auth.data.get('article_id', '')
        if auth_article_id == article_id: 
            if auth_version != auth.data.get('authoringVersion', -1):
                authoring_version['Body'][mid].data['authoringVersion'] = auth_version
                return "MODIFIED"
            else:
                return "UNCHANGED"
        elif auth_article_id < article_id:
            left = mid + 1
        else:
            right = mid - 1

    return 'NEW'

def get_next_article_bundle(bundle_size = 100, pages = 2):
    total_articles, articles_parsed = get_total_article_count(), get_articles_parsed_count()
    
    for i in range(pages):
        if bundle_size + articles_parsed > total_articles:
            bundle_size = total_articles - articles_parsed
        
        result = urllib.request.urlopen(urllib.request.Request(
            url= environ['AIH_HEALTHWISE_INVENTORY_URL'] + '?top=' + str(bundle_size) + '&skip=' + str(articles_parsed),
            headers={'Accept': 'application/json','Authorization':bearer_token},
            method='GET'))
        raw_data = result.read()
        encoding = result.info().get_content_charset('utf8')  # JSON default
        data = json.loads(raw_data.decode(encoding))
        inventory = data['data']['inventory']
        yield [(item['hwid'], item["href"], item['type'], item['authoringVersion']) for item in inventory]
        articles_parsed += bundle_size
           
def transform(data):
    article = Article(data)
    # article.update_all()
    return json.dumps(article.data, indent = 4)
    # return json.dumps(data, indent = 4) 
    # it is simply converting the dict back to json for storing.
 
def get_articles_parsed_count():
    # Open S3 bucket and try to find Parse_Count.json
    FILE_NAME = 'Parse_Count.json'
    data = retriveObjfromS3(FILE_NAME, AWS_BUCKET_NAME_LOGS) 
    if type(data) != list:
        # data = []
        # print("PITFALL")
        # # save_to_s3(0, json.dumps(data, indent = 4), file_name=FILE_NAME, bucket_name=AWS_BUCKET_NAME_LOGS)
        return 0

    else:
        result = data[-1].get('Records_Parsed', -1)
        
        if result == get_total_article_count():
            result = 0
        # Initiates the batch processing all over again from 0
        return  result

def update_articles_parsed_count(total_count, articles_parsed, response):
    FILE_NAME = 'Parse_Count.json'
    BUCKET_NAME = AWS_BUCKET_NAME_LOGS
    BUFFER_TIME_IN_MINS = 1
    
    # This S3 Bucket is supposed to have a trigger to call this lambda function again.

    data = retriveObjfromS3(FILE_NAME, BUCKET_NAME) # Only receives the body which is a list
    if type(data) != list:
        data = []  
        log_obj = {
            'time_stamp': str(datetime.datetime.now()),
            'Records_Parsed': 0,
            'Total_Count': total_count,
            'FAILED(Old, New)-UNCHANGED-NEW': '(0, 0)-0-0',
            'Correctness': 'TRUE',
            'Completion Percentage': '0%',
            'Completion Status': 'IN-PROGRESS',
        }
        data.append(log_obj)
    
    last_data = data[-1]
    
    # Boundary Condition for Termination
    if last_data.get('Completion Status', '') == "COMPLETED":
        now_time = datetime.datetime.now()
        last_time = datetime.datetime.strptime(last_data.get('time_stamp'), '%Y-%m-%d %H:%M:%S.%f')
        if now_time < last_time + datetime.timedelta(minutes=BUFFER_TIME_IN_MINS):
            print("Completed Successfully")
            return
    data.append(response.get('body', {}))

    if len(data) > 200:
        data = data[-50:]
    # This line will save obj to log bucket which will trigger this lambda in turn.``
    save_to_s3(0, json.dumps(data, indent = 4), file_name=FILE_NAME, bucket_name=BUCKET_NAME)
    print("TRIGGERED!!!")

def get_total_article_count():
    
    # Open S3 bucket and try to find Parse_Count.json
    
    FILE_NAME = 'Parse_Count.json'
    parse_count_obj = retriveObjfromS3(FILE_NAME, AWS_BUCKET_NAME_LOGS)
    if type(parse_count_obj) == dict:
        result = urllib.request.urlopen(urllib.request.Request(
            url= environ['AIH_HEALTHWISE_INVENTORY_URL'] + '?top=1&skip=0',
            headers={'Accept': 'application/json','Authorization':bearer_token},
            method='GET'))
        raw_data = result.read()
        encoding = result.info().get_content_charset('utf8')  # JSON default
        data = json.loads(raw_data.decode(encoding))
        pagination = data['data']['pagination']
        total_record_count = pagination['total']
        return total_record_count

    else:
        return parse_count_obj[-1].get('Total_Count', -1)

def get_article_dict_from_href(href):
    headers={'Accept': 'application/json','Authorization':bearer_token, "X-HW-Version": "2"}
    markDownJsonQuery = '?contentOutput=markdown+json'
    href += markDownJsonQuery
    result = requests.request("GET", href, headers=headers)

    if result.status_code == 200:
        data = json.loads(result.text)
        return data
        # returns a dict object
    else:
        return "Article could not be fetched from Healthwise."

def write_to_s3(AWS_BUCKET_NAME, file_name, json_obj):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(AWS_BUCKET_NAME)
    bucket.put_object(
        ContentType='application/json',
        Key=file_name,
        Body=json_obj,
    )

def save_to_s3(article_id, json_obj, file_name = "NOT_RECEIVED", bucket_name="NOT_RECEIVED"):
    AWS_BUCKET_NAME = bucket_name if bucket_name != "NOT_RECEIVED" else environ['AWS_BUCKET_NAME']
    if file_name == "NOT_RECEIVED":
        file_name = str(article_id) + ".json"
    try:
        write_to_s3(AWS_BUCKET_NAME, file_name, json_obj)
        body = {
            "uploaded": "true",
            "bucket": AWS_BUCKET_NAME,
            "JSON obj name: ": file_name,
        }
        return {
            "statusCode": 200,
            "body": json.dumps(body)
        }
    except Exception as e:
        print(str(e), "--")
        return {
            "statusCode": 403,
            "body": {
                'uploaded': False,
                'ERR_code': "Access_Denied",
                "bucket": AWS_BUCKET_NAME,
                'JSON obj name:': file_name,
            }
        }
#
def read_from_s3(file_name, bucket_name):
    s3_client = boto3.client("s3")
    file_content = s3_client.get_object(Bucket=bucket_name, Key=file_name)['Body']  # different
    return json.loads(file_content.read())

def retriveObjfromS3(file_name, bucket_name):
    try:
        return read_from_s3(file_name, bucket_name)
    except Exception as e:
        print(str(e), file_name, bucket_name)
        return {
            "status": "Failure",
            "ErrorCode": 403
        }

def write_failed_ids_list_to_s3(failed_ids):
    AWS_BUCKET_NAME = environ['AWS_BUCKET_NAME']
    failed_ids['count'] = len(failed_ids.get('Body', []))
    data = {
        'count': failed_ids['count'],
        'Body': [obj.data for obj in failed_ids['Body']]
    }
    try:
        write_to_s3(AWS_BUCKET_NAME, 'Failed_Ids.json', json.dumps(data, indent = 4))
        return json.dumps({
            "status": "Success",
            "status_code": 200
        })
    except Exception as e:
        print("Uploading Failed IDs failed", str(e))
        return json.dumps({
            "status": "Failed",
            "status_code": 403
        })

def read_failed_ids_list_from_s3():
    failed_ids =  retriveObjfromS3("Failed_Ids.json", AWS_BUCKET_NAME)
    if failed_ids.get('ErrorCode', '') == 403:
        # if "Failed_Ids.json" file does not exist in the bucket, do the initialization
        data = {
            'count': 0,
            'Body': []
        } 
        write_failed_ids_list_to_s3(data)
        return data
    else:
        return {
            'count': failed_ids['count'],
            'Body': [FailedID(x['article_id'], x['article_type'], x['article_href']) for x in failed_ids['Body']]
        }

def get_authoring_version():
    FILE_NAME = 'Authoring_Version.json'
    AWS_BUCKET_NAME = environ['AWS_BUCKET_NAME']
    authoring_version =  retriveObjfromS3(FILE_NAME, AWS_BUCKET_NAME)
    if authoring_version.get('ErrorCode', '') == 403:
        # if "Authoring_Version.json" file does not exist in the bucket, do the initialization
        data = {
            'count': 0,
            'Body': [],
        }
        write_authoring_version(data)
        return data
    else:
        return {
            'count': authoring_version['count'],
            'Body': [AuthVersion(x['article_id'], x['authoringVersion']) for x in authoring_version['Body']]
        }

def write_authoring_version(authoring_version):
    AWS_BUCKET_NAME = environ['AWS_BUCKET_NAME']
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(AWS_BUCKET_NAME)
    authoring_version['count'] = len(authoring_version.get('Body', []))
    data = {
        'count': authoring_version['count'],
        'Body': [obj.data for obj in authoring_version['Body']]
    }

    try:
        write_to_s3(AWS_BUCKET_NAME, 'Authoring_Version.json', json.dumps(data, indent = 4))
        return {
            "status": "Success",
            "status_code": 200,
            'count': authoring_version['count'],
        }
    except Exception as e:
        print("Writting authoring version failed", str(e))
        return {
            "status": "Failed",
            "status_code": 403,
            'count': None,
        }

@functools.total_ordering
class AuthVersion:
    def __init__(self, article_id, version):
        self.data = {
            'article_id': article_id,
            'authoringVersion': version
        }
    def __lt__(self, other):
        return self.data['article_id'] < other.data['article_id']
    
    def __str__(self):
        return str(self.data)

@functools.total_ordering
class FailedID:
    def __init__(self, article_id, article_type, href):
        self.data = {
            'article_id': article_id, 
            'article_type': article_type, 
            'article_href': href
        }

    def __lt__(self, other):
        return self.data['article_id'] < other.data['article_id']
    
    def __str__(self):
        return str(self.data)

def lambda_handler(event, context):
    failed_ids = read_failed_ids_list_from_s3()
    authoring_version = get_authoring_version()
    articles_parsed, failed_id_count, modified_id_count = 0, 0, 0
    unchanged_id_count, new_id_count, failed_id_count_now =  0, 0, 0
    
    for bundle in get_next_article_bundle(bundle_size = 100, pages = 5):
        for article_id, href, article_type, auth_version in bundle:
            article_state = get_article_state(article_id, auth_version, authoring_version, failed_ids)
            if article_state == 'FAILED':
                failed_id_count += 1
            elif article_state =='UNCHANGED':
                unchanged_id_count += 1
            elif article_state in ('MODIFIED', 'NEW'):
                if article_state == 'MODIFIED':
                    modified_id_count += 1
                article = get_article_dict_from_href(href)
                if type(article) == str:  # if article cannot be fetched from Healthwise 
                    failed_id = FailedID(article_id, article_type, href)
                    bisect.insort_left(failed_ids['Body'], failed_id)
                    # Ensures that the list is sorted even after insertion
                    failed_ids['count'] += 1
                    articles_parsed += 1
                    failed_id_count_now += 1
                    continue
                new_json_obj = transform(article)
                save_to_s3(article_id, new_json_obj)
                if article_state == 'NEW':
                    new_id_count += 1
                    auth_obj = AuthVersion(article_id, auth_version)
                    bisect.insort_left(authoring_version['Body'], auth_obj)
                    # Ensures that the list is sorted even after insertion
                    authoring_version['count'] += 1
            articles_parsed += 1

    total_records_parsed = get_articles_parsed_count() + articles_parsed
    total_article_count = get_total_article_count()
    
    if failed_id_count_now != 0:
        write_failed_ids_list_to_s3(failed_ids)
    if any([modified_id_count, new_id_count]):
        write_authoring_version(authoring_version)

    total_failed_ids_count = failed_ids.get('count', -1)
    # total_success_id_count = authoring_version.get('count', -1)
    response = {
        'statusCode': 200,
        'body': {
            'time_stamp': str(datetime.datetime.now()),
            'Records_Parsed': total_records_parsed,
            'Total_Count': total_article_count,
            'FAILED(Old, New)-UNCHANGED-NEW': f'({failed_id_count},{failed_id_count_now})-{unchanged_id_count}-{new_id_count}',
            'Correctness': 'TRUE' if failed_id_count + failed_id_count_now + unchanged_id_count+ new_id_count == articles_parsed else 'FALSE',
            'Completion Percentage': str(total_records_parsed / total_article_count * 100) + '%',
            'Completion Status': 'IN-PROGRESS' if total_records_parsed != total_article_count else 'COMPLETED',
        }
    }

    update_articles_parsed_count(total_article_count, total_records_parsed, response)
    print(response)
    return response
