import unittest
from unittest import mock
import json

# Setting the default AWS region environment variable required by the Python SDK boto3
with mock.patch.dict('os.environ', {'AWS_REGION': 'us-east-1'}):
    from lambda_function import lambda_handler, Article, get_article_dict_from_href, environ


from contentFiles import failed_ids, correct_ids

def mocked_get_next_article_bundle(bundle_size = 5, pages = 2):
    with open('../InventoryData/inventoryData.json') as json_file:
        x = json.load(json_file)
        y = x['data']['inventory']
        z = [(item['hwid'], item['href'], item['type'], item['authoringVersion']) for item in y]
        k = [
            z[:5],
            z[5:]
        ]
        for i in range(2):
            yield k[i]

def mocked_get_article_dict_from_href(href):
    if type(href) == str:
        prefix = 'https://content.healthwise.net/articles/'
        if href.startswith(prefix):
            uuid = href[len(prefix):-6]
            if uuid in correct_ids:
                with open(f'../ContentFilesRaw/{uuid}_u.json') as json_file:
                    return json.load(json_file)
    print("Failed ID Detected: ", href)       
    return "Article could not be fetched from Healthwise."

def mocked_write_to_s3(AWS_BUCKET_NAME, file_name, json_obj):
    with open(file_name, "w") as outfile:
        outfile.write(json_obj)

def mocked_read_from_s3(file_name, bucket_name):
    with open(file_name, 'r') as openfile:
        dict_object = json.load(openfile)
        return dict_object

def mocked_get_total_article_count():
    return len(correct_ids) + len(failed_ids)


class UnitTestCase(unittest.TestCase):
    @mock.patch('lambda_function.get_total_article_count', side_effect= mocked_get_total_article_count)
    @mock.patch('lambda_function.get_next_article_bundle', side_effect= mocked_get_next_article_bundle)
    @mock.patch('lambda_function.get_article_dict_from_href', side_effect= mocked_get_article_dict_from_href)
    @mock.patch('lambda_function.write_to_s3', side_effect= mocked_write_to_s3)
    @mock.patch('lambda_function.read_from_s3', side_effect= mocked_read_from_s3)
    def test_content_transformation(self, readS3, writeS3, dictFromHREF, nxtBundle, articleCount):
        # response = lambda_handler(None, None)
        # self.assertEqual(dictFromHREF.call_count, mocked_get_total_article_count())
        for article in correct_ids:
            with open(article + '.json') as jsonFile:
                response = json.load(jsonFile)
                with open(f'../ContentFiles/{article}_exp.json') as jsonFile2:
                    expected_response = json.load(jsonFile2)
                    self.assertDictEqual(response, expected_response)



    @mock.patch('lambda_function.get_total_article_count', side_effect= mocked_get_total_article_count)
    @mock.patch('lambda_function.get_next_article_bundle', side_effect= mocked_get_next_article_bundle)
    @mock.patch('lambda_function.get_article_dict_from_href', side_effect= mocked_get_article_dict_from_href)
    @mock.patch('lambda_function.write_to_s3', side_effect= mocked_write_to_s3)
    @mock.patch('lambda_function.read_from_s3', side_effect= mocked_read_from_s3)
    def test_metadata(self, readS3, writeS3, dictFromHREF, nxtBundle, articleCount):
        files = ['Parse_Count', 'Authoring_Version', 'Failed_Ids']
        for file in files:
            with open(f'{file}.json') as jsonFile:
                response = json.load(jsonFile)
                with open(f'../MetaData/{file}_Final.json') as jsonFile2:
                    expected_response = json.load(jsonFile2)
                    if file == 'Parse_Count':
                        for res in response:
                            res.pop('time_stamp')
                        for res in expected_response:
                            res.pop('time_stamp')
                        self.assertEqual(response, expected_response)
                    else:
                        self.assertDictEqual(response, expected_response)

    @mock.patch('lambda_function.get_total_article_count', side_effect= mocked_get_total_article_count)
    @mock.patch('lambda_function.get_next_article_bundle', side_effect= mocked_get_next_article_bundle)
    @mock.patch('lambda_function.get_article_dict_from_href', side_effect= mocked_get_article_dict_from_href)
    @mock.patch('lambda_function.write_to_s3', side_effect= mocked_write_to_s3)
    @mock.patch('lambda_function.read_from_s3', side_effect= mocked_read_from_s3)
    def test_parse_correctness(self, readS3, writeS3, dictFromHREF, nxtBundle, articleCount):
        file_handle = open('Parse_Count.json')
        response = all([record['Correctness'] for record in json.load(file_handle)])
        file_handle.close()
        self.assertTrue(response)
    


if __name__ == '__main__':
    unittest.main()

    
