import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import joblib

# Load your CSV file
df = pd.read_csv('historical_events.csv')

# Features and label
X = df[['price', 'volume', 'rvol', 'prepost', 'event_type_code']]
y = df['is_runner']

# Train the model
clf = RandomForestClassifier(n_estimators=100, random_state=42)
clf.fit(X, y)

# Save the model as runner_model.joblib
joblib.dump(clf, 'runner_model.joblib')

print("Model trained and saved as runner_model.joblib!")