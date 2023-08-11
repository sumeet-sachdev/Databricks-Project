import json
import random
from datetime import datetime, timedelta

 
# Function to generate a random timestamp

def random_timestamp(start, end):
    time_format = '%Y-%m-%d %H:%M:%S'
    start_timestamp = datetime.strptime(start, time_format)
    end_timestamp = datetime.strptime(end, time_format)
    delta = end_timestamp - start_timestamp
    random_seconds = random.randint(0, delta.total_seconds())
    return (start_timestamp + timedelta(seconds=random_seconds)).strftime(time_format)

 

# Generate sample web logs

logs = []
for log_id in range(1, 50000):  # Generate log entries
    action = random.choice(["view", "add_to_cart", "purchase"])
    if action == "view":
        quantity = None
    else:
        quantity = random.randint(1, 10)
    log_entry = {
        "log_id": log_id,
        "timestamp": random_timestamp("2023-08-02 00:00:00", "2023-08-02 23:59:59"),
        "customer_id": f"CUST{random.randint(1, 10000)}",
        "product_id": f"PROD{random.randint(1, 50):03d}",
        "action": action,
        "quantity": quantity,

    }

    logs.append(log_entry)


# Save the web logs to a JSON file

with open("web_logs.json", "w") as f:
    json.dump(logs, f, indent=2)