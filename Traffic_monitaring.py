import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


file_path=r"C:\Users\2022\Desktop\Praneeth\New project\Traffic_Monitoring_System_Dataset.csv"
traffic_data=pd.read_csv(file_path)


def clean_data(df):
    df = df.drop_duplicates()
    df = df.dropna()
    return df

trafic_conditions=clean_data(traffic_data)
print(trafic_conditions.head())


def save_to_excel(df, filename):
    df.to_excel(filename, index=False)
    print("File saved to Excel successfully")

save_to_excel(trafic_conditions, "python_output.xlsx")

def analyze_vehical_count(df):
    avg_vehicle_count = df.groupby('Route_ID')['Vehicle_Count'].mean()
    return avg_vehicle_count

analysed_count=analyze_vehical_count(trafic_conditions)
print(analysed_count.head())

def analyze_vehical_speed(df):
    avg_speed = df.groupby('Route_ID')['Avg_Speed'].mean()
    return avg_speed

analysed_speed=analyze_vehical_speed(trafic_conditions)

print(analysed_count.head())
print(analysed_speed.head())

correlation = trafic_conditions[['Vehicle_Count', 'Visibility', 'Temperature', 'Humidity', 'Wind_Speed']].corr()
vehicle_count_correlation = pd.DataFrame(correlation.loc['Vehicle_Count']).transpose()
vehicle_count_correlation = vehicle_count_correlation.drop(columns=['Vehicle_Count'])
print(vehicle_count_correlation)


def analyze_location(dataframe, location):
    # Trim any leading/trailing whitespace from location parameter
    location = location.strip()
    
    # Ensure 'Timestamp' column is in datetime format
    dataframe['Timestamp'] = pd.to_datetime(dataframe['Timestamp'])
    
    # Filter data for the specific location
    location_data = dataframe[dataframe['Area'] == location].copy()  # Make a copy to avoid SettingWithCopyWarning
    
    if location_data.empty:
        return f"No data available for location: {location}", None, None
    
    # Extract time from Timestamp column
    location_data['Time'] = location_data['Timestamp'].dt.strftime('%H:%M') 
    
    # Find the time with the least and most vehicles
    best_time = location_data.loc[location_data['Vehicle_Count'].idxmin(), 'Time']
    worst_time = location_data.loc[location_data['Vehicle_Count'].idxmax(), 'Time']
    
    # Check if the number of accidents is high
    accidents_threshold = 5  # Define what "high" means
    total_accidents = location_data['Accidents'].sum()
    accident_message = "Accident zone, be careful!" if total_accidents > accidents_threshold else "No significant accidents."
    
    return {
        'Best time to travel': best_time,
        'Worst time to travel': worst_time,
        'Accident message': accident_message
    }


# Access the returned values
best_time_analysis = analyze_location(trafic_conditions,"Ameerpet")
print(best_time_analysis)

def calculate_bus_frequency(df):
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%d-%m-%Y %H:%M')
    df['TimeDiff'] = df['Timestamp'].diff()
    df.loc[df.index[0], 'TimeDiff'] = pd.Timedelta(hours=1)
    df['Bus_Frequency_per_Minute'] = df['Vehicle_Count'] / df['TimeDiff'].dt.total_seconds() * 60
    df.loc[df.index[0], 'Bus_Frequency_per_Minute'] = df.loc[df.index[0], 'Vehicle_Count'] / 60
    df['Bus_Frequency_per_Minute'] = df['Bus_Frequency_per_Minute'].round(2)
    new_df = df[['Timestamp', 'Vehicle_Count', 'TimeDiff', 'Bus_Frequency_per_Minute','Accidents']].copy()
    new_df.sort_values(by='Bus_Frequency_per_Minute', ascending=False, inplace=True)

    return new_df

result_df = calculate_bus_frequency(trafic_conditions)
print(result_df)


def calculate_accident_frequency(df):
    # Calculate accident frequency as accidents per minute
    df['Accident_Frequency'] = df['Accidents'] / (df['TimeDiff'].dt.total_seconds() / 60)
    
    # Round the accident frequency to 2 decimal places
    df['Accident_Frequency'] = df['Accident_Frequency'].round(2)
    
    # Select and sort relevant columns
    new_df = df[['Timestamp', 'Accidents', 'TimeDiff', 'Accident_Frequency']].copy()
    new_df.sort_values(by='Accident_Frequency', ascending=False, inplace=True)
    
    return new_df
Accedent_df=calculate_accident_frequency(result_df)
print(Accedent_df)

def plot_combined_data(df):
    # Calculate average duration for each Route ID
    avg_duration = df.groupby('Route_ID')['TimeDiff'].mean()
    
    plt.figure(figsize=(15, 12))  # Adjust figure size as needed
    
    # Bar chart for Average Duration
    plt.subplot(3, 2, 1)
    avg_duration.plot(kind='line', color='skyblue')
    plt.xlabel('Route ID')
    plt.ylabel('Average Duration')
    plt.title('Average Duration of Bus Routes per Route ID')
    plt.xticks(rotation=90)
    
    # Scatter plot for Visibility vs Vehicle Count
    plt.subplot(3, 2, 3)
    sns.lineplot(x='Visibility', y='Vehicle_Count', data=df)
    plt.title('Visibility vs Vehicle Count')

    # Scatter plot for Temperature vs Vehicle Count
    plt.subplot(3, 2, 4)
    sns.lineplot(x='Temperature', y='Vehicle_Count', data=df)
    plt.title('Temperature vs Vehicle Count')

    # Scatter plot for Humidity vs Vehicle Count
    plt.subplot(3, 2, 5)
    sns.lineplot(x='Humidity', y='Vehicle_Count', data=df)
    plt.title('Humidity vs Vehicle Count')

    # Scatter plot for Wind Speed vs Vehicle Count
    plt.subplot(3, 2, 6)
    sns.lineplot(x='Wind_Speed', y='Vehicle_Count', data=df)
    plt.title('Wind Speed vs Vehicle Count')
    
    # Line plot for both Bus Frequency per Minute and Accident Frequency
    plt.subplot(3, 2, 2)
    sns.barplot(x='Timestamp', y='Bus_Frequency_per_Minute', data=result_df, label='Bus Frequency per Minute')
    sns.barplot(x='Timestamp', y='Accident_Frequency', data=Accedent_df, label='Accident Frequency')
    plt.title('Bus and Accident Frequency')
    plt.xlabel('Timestamp')
    plt.ylabel('Frequency')
    plt.legend()
    
    plt.tight_layout()
    plt.show()

# Assuming 'trafic_conditions' is your DataFrame containing necessary columns
plot_combined_data(trafic_conditions)


