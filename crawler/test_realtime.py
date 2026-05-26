

import sys

def test_elasticsearch():
    """Test Elasticsearch connection."""
    try:
        from elasticsearch import Elasticsearch
        es = Elasticsearch("http://localhost:9200")
        if es.ping():
            print("Elasticsearch: Connected")
            # Check if index exists
            if es.indices.exists(index="phapluat"):
                count = es.count(index="phapluat")["count"]
                print(f"  - Index 'phapluat' exists with {count} documents")
            else:
                print("  - Index 'phapluat' does not exist yet")
            return True
        else:
            print("✗ Elasticsearch: Cannot connect")
            return False
    except ImportError:
        print("Elasticsearch: Module not installed (pip install elasticsearch)")
        return False
    except Exception as e:
        print(f"✗ Elasticsearch: Error - {e}")
        return False


def test_kafka():
    """Test Kafka connection."""
    try:
        from confluent_kafka import Producer
        producer = Producer({"bootstrap.servers": "localhost:29092"})

        metadata = producer.list_topics(timeout=5)
        print("Kafka: Connected")
        topics = metadata.topics
        if "van-ban-phap-luat" in topics:
            print("  - Topic 'van-ban-phap-luat' exists")
        else:
            print("  - Topic 'van-ban-phap-luat' does not exist (will be auto-created)")
        return True
    except ImportError:
        print("Kafka: Module not installed (pip install confluent-kafka)")
        return False
    except Exception as e:
        print(f"Kafka: Error - {e}")
        return False


def test_minio():
    """Test MinIO connection."""
    try:
        from minio import Minio
        client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )

        if client.bucket_exists("phapluat"):
            print("MinIO: Connected")
            print("  - Bucket 'phapluat' exists")
            return True
        else:
            print("MinIO: Connected")
            print("  - Bucket 'phapluat' does not exist (will be created)")
            return True
    except ImportError:
        print("MinIO: Module not installed (pip install minio)")
        return False
    except Exception as e:
        print(f"✗ MinIO: Error - {e}")
        return False


def main():
    print("Testing Realtime Crawler Setup\n" + "="*40)
    
    results = []
    results.append(test_elasticsearch())
    results.append(test_kafka())
    results.append(test_minio())
    
    print("\n" + "="*40)
    if all(results):
        print("All services are ready!")
        print("\nYou can now run:")
        print("  python playwright_scrape.py --start 1 --end 2 --realtime")
        sys.exit(0)
    else:
        print("Some services are not ready")
        print("\nMake sure to start services:")
        print("  bash k8s/start.sh")
        sys.exit(1)


if __name__ == "__main__":
    main()
