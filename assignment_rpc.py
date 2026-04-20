import requests
import time
import collections
from multiprocessing import Pool, cpu_count

# Configuration
BASE_URL = "http://72.60.221.150:8080"
STUDENT_ID = "MDS202521"

MAX_RETRIES = 5
RETRY_BACKOFF = 1.0  # seconds to wait on first retry (doubles each time)


def login(student_id):
    """Log in and retrieve a dynamic secret key."""
    response = requests.post(
        f"{BASE_URL}/login",
        json={"student_id": student_id},
        timeout=10
    )
    response.raise_for_status()
    return response.json()["secret_key"]


def get_publication_title(student_id, filename):
    """
    1. Log in to get a dynamic SHA256 secret key.
    2. Use the key to retrieve the publication title for the given filename.
    3. Handle 429 (Too Many Requests) by implementing exponential backoff retry.
    4. Handle other errors (404, 500, etc.) gracefully.
    """
    for attempt in range(MAX_RETRIES):
        try:
            secret_key = login(student_id)

            response = requests.post(
                f"{BASE_URL}/lookup",
                json={"secret_key": secret_key, "filename": filename},
                timeout=10
            )

            if response.status_code == 429:
                wait_time = RETRY_BACKOFF * (2 ** attempt)
                print(f"[429] Rate limited on {filename}. Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue

            if response.status_code == 404:
                print(f"[404] {filename} not found. Skipping.")
                return None

            response.raise_for_status()
            return response.json().get("title", "")

        except requests.exceptions.RequestException as e:
            wait_time = RETRY_BACKOFF * (2 ** attempt)
            print(f"[ERROR] {filename} attempt {attempt + 1} failed: {e}. Retrying in {wait_time:.1f}s...")
            time.sleep(wait_time)

    print(f"[FAILED] Could not retrieve {filename} after {MAX_RETRIES} attempts.")
    return None


def mapper(filename_chunk):
    """
    Map phase: takes a chunk (list) of filenames, fetches each title,
    extracts the first word, and returns a Counter of first-word frequencies.
    Each worker logs in independently per request to get a fresh key.
    """
    word_counts = collections.Counter()

    for filename in filename_chunk:
        title = get_publication_title(STUDENT_ID, filename)
        if title:
            first_word = title.strip().split()[0] if title.strip() else None
            if first_word:
                word_counts[first_word] += 1

    return word_counts


def reducer(counters):
    """
    Reduce phase: merge all Counter dicts from workers into one.
    """
    total = collections.Counter()
    for counter in counters:
        total.update(counter)
    return total


def verify_top_10(student_id, top_10_list):
    """
    1. Log in to get a dynamic SHA256 secret key.
    2. Submit the top_10_list to the /verify endpoint.
    3. Print the final score and message from the server.
    """
    secret_key = login(student_id)
    response = requests.post(
        f"{BASE_URL}/verify",
        json={"secret_key": secret_key, "top_10": top_10_list},
        timeout=10
    )
    response.raise_for_status()
    result = response.json()
    print("\n===== VERIFICATION RESULT =====")
    print(f"Score   : {result.get('score')} / {result.get('total')}")
    print(f"Correct : {result.get('correct')}")
    print(f"Message : {result.get('message')}")
    print("================================\n")
    return result


def chunkify(lst, n):
    """Split list into n roughly equal chunks."""
    k, remainder = divmod(len(lst), n)
    chunks = []
    start = 0
    for i in range(n):
        end = start + k + (1 if i < remainder else 0)
        chunks.append(lst[start:end])
        start = end
    return chunks


if __name__ == "__main__":
    # Step 1: Build the list of all filenames
    filenames = [f"pub_{i}.txt" for i in range(1000)]

    # Step 2: Determine number of workers
    num_workers = min(cpu_count(), 8)  # cap at 8 to respect rate limits
    print(f"Using {num_workers} worker processes for {len(filenames)} files.\n")

    # Step 3: Divide filenames into chunks (one chunk per worker)
    chunks = chunkify(filenames, num_workers)

    # Step 4: Map phase — parallel title retrieval and word counting
    print("Starting Map phase...")
    with Pool(processes=num_workers) as pool:
        partial_counts = pool.map(mapper, chunks)

    # Step 5: Reduce phase — merge all partial counters
    print("\nStarting Reduce phase...")
    total_counts = reducer(partial_counts)

    # Step 6: Extract Top 10 most frequent first words
    top_10 = [word for word, count in total_counts.most_common(10)]

    print("\n===== TOP 10 MOST FREQUENT FIRST WORDS =====")
    for rank, (word, count) in enumerate(total_counts.most_common(10), start=1):
        print(f"  {rank:>2}. {word:<20} ({count} occurrences)")
    print("=============================================\n")

    # Step 7: Verify with the server
    if top_10:
        verify_top_10(STUDENT_ID, top_10)
    else:
        print("No words found — check your network connection and student ID.")