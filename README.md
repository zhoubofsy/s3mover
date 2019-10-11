s3mover.py
===========================

Script to copy all files from an S3 user to another user in a different cluster. using many threads in Python.

# Usage

## Install dependence package

``` shell
pip install -U boto
```

## Configuration

Edit `users.json` file.

Example:
``` json
[
    {
        "from":{
            "host":"10.100.13.111",
            "port":7480,
            "access":"98KJGUYTFVHB09JGHHB0",
            "secret":"A4JuvB468jsdfskjliGVDBEHYfiEVBNMNBVCVBNc"
        },
        "to":{
            "host":"10.100.13.112",
            "port":7480,
            "access":"8E439FJK875K3UFLQUB0",
            "secret":"BNHJvB468tmnDpmkZMfwesbyuihhjknbnmnhjjhh"
        },
        "bucketmap":{
            "a-test-bkt":"new-a-test-bkt",
            "dest-bkt":"new-dest-bkt"
        }
    },
    {
        "from":{
            "host":"10.100.13.111",
            "port":7480,
            "access":"1987RTYUNJI90UFLQUB0",
            "secret":"YUUfghjbnm345678bnmwesb2zmGZeSjjhjjhjkjh"
        },
        "to":{
            "host":"10.100.13.112",
            "port":7480,
            "access":"567GHJGHJKJH45RTYUNB",
            "secret":"QWERJHJJNKKMqwejdlgkj345657ljlkjljYUIYNN"
        }
    }
]
```

## Run

``` shell
./s3mover.sh
```

or

``` shell
python s3mover.py
```

When dealing with a very large number of files, you might want to try to use more worker threads:

``` shell
python s3mover.py -t 100
```

# Thanks

* [s3_bucket_to_bucket_copy_py](https://github.com/paultuckey/s3_bucket_to_bucket_copy_py)
* [Bucket Restrictions and Limitations](https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html)

