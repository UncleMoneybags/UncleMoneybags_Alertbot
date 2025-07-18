import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import joblib

# Load your labeled data
df = pd.read_csv("event_log_labeled.csv")

# Example: using price and size as features
X = df[['price', 'size']]
y = df['is_runner']

# Train the model
clf = RandomForestClassifier()
clf.fit(X, y)

# Save the trained model
joblib.dump(clf, "runner_model.joblib")
print("Model saved as runner_model.joblib")
