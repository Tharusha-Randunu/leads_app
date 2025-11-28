# helpers/metric_calculator.py
import pandas as pd
import os
import numpy as np
import re
from datetime import datetime, timedelta

class MetricsCalculator:
    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    def generate_call_analysis_table(self, call_logs_df):
        """Generate call analysis table - ONE ROW PER PHONE NUMBER"""
        if call_logs_df.empty:
            return pd.DataFrame()
        
        print("📊 Generating call analysis table from call logs...")
        
        analysis_data = []
        
        # Group by cleaned phone number
        for phone in call_logs_df['phone_cleaned'].unique():
            # Get all call records for this phone number
            phone_calls = call_logs_df[call_logs_df['phone_cleaned'] == phone]
            
            if phone_calls.empty:
                continue
            
            # Calculate metrics for this phone number
            metrics = self._calculate_phone_metrics(phone_calls, phone)
            analysis_data.append(metrics)
        
        # Create final dataframe
        analysis_df = pd.DataFrame(analysis_data)
        
        print(f"✅ Generated analysis for {len(analysis_df)} unique phone numbers")
        return analysis_df
    
    def _calculate_phone_metrics(self, phone_calls, phone):
        """Calculate all metrics for a specific phone number from call logs"""
        metrics = {
            'phone': phone,
            'no_of_times_called': len(phone_calls),
        }
        
        # Get name if available (most common name from call logs)
        metrics['name'] = self._get_most_common_name(phone_calls)
        
        # Calculate date-based metrics
        date_metrics = self._calculate_date_metrics(phone_calls)
        metrics.update(date_metrics)
        
        # Calculate time-based metrics
        time_metrics = self._calculate_time_metrics(phone_calls)
        metrics.update(time_metrics)
        
        # Get all dates and times called
        metrics['dates_times_called'] = self._get_all_dates_times(phone_calls)
        
        return metrics
    
    def _get_most_common_name(self, phone_calls):
        """Get the most common name for this phone number from call logs"""
        if 'name' in phone_calls.columns:
            names = phone_calls['name'].dropna()
            if not names.empty:
                return names.mode()[0] if not names.mode().empty else names.iloc[0]
        return "Unknown"
    
    def _calculate_date_metrics(self, phone_calls):
        """Calculate date-based metrics like average gap between calls"""
        metrics = {}
        
        # Find date column
        date_col = self._find_date_column(phone_calls)
        if date_col:
            dates = pd.to_datetime(phone_calls[date_col], errors='coerce').dropna()
            
            if len(dates) >= 2:
                dates_sorted = dates.sort_values()
                # Calculate gaps between consecutive calls (in days)
                gaps = (dates_sorted.diff().dropna()).dt.total_seconds() / (24 * 3600)  # Convert to days
                
                metrics['avg_gap_between_calls'] = round(gaps.mean(), 2)
                metrics['min_gap_between_calls'] = round(gaps.min(), 2)
                metrics['max_gap_between_calls'] = round(gaps.max(), 2)
                metrics['first_call_date'] = dates_sorted.min().strftime('%Y-%m-%d %H:%M:%S')
                metrics['last_call_date'] = dates_sorted.max().strftime('%Y-%m-%d %H:%M:%S')
                metrics['total_call_days'] = len(dates_sorted.dt.date.unique())
            elif len(dates) == 1:
                single_date = dates.iloc[0]
                metrics['avg_gap_between_calls'] = 0
                metrics['min_gap_between_calls'] = 0
                metrics['max_gap_between_calls'] = 0
                metrics['first_call_date'] = single_date.strftime('%Y-%m-%d %H:%M:%S')
                metrics['last_call_date'] = single_date.strftime('%Y-%m-%d %H:%M:%S')
                metrics['total_call_days'] = 1
            else:
                metrics.update(self._get_default_date_metrics())
        else:
            metrics.update(self._get_default_date_metrics())
        
        return metrics
    
    def _calculate_time_metrics(self, phone_calls):
        """Calculate time-based metrics like total call time"""
        metrics = {}
        
        # Find duration column
        duration_col = self._find_duration_column(phone_calls)
        if duration_col:
            # Parse all durations to seconds
            total_seconds = 0
            valid_durations = 0
            
            for duration in phone_calls[duration_col].dropna():
                seconds = self._parse_duration_to_seconds(duration)
                if seconds > 0:
                    total_seconds += seconds
                    valid_durations += 1
            
            if valid_durations > 0:
                metrics['total_time_spent_seconds'] = total_seconds
                metrics['avg_time_per_call_seconds'] = round(total_seconds / valid_durations, 2)
                metrics['total_time_spent'] = self._format_duration(total_seconds)
                metrics['avg_time_per_call'] = self._format_duration(metrics['avg_time_per_call_seconds'])
            else:
                metrics.update(self._get_default_time_metrics())
        else:
            metrics.update(self._get_default_time_metrics())
        
        return metrics
    
    def _get_all_dates_times(self, phone_calls):
        """Get all dates and times when this number was called"""
        date_col = self._find_date_column(phone_calls)
        if date_col:
            dates = pd.to_datetime(phone_calls[date_col], errors='coerce').dropna()
            if not dates.empty:
                # Format as readable dates and times
                formatted_dates = dates.dt.strftime('%Y-%m-%d %H:%M:%S').tolist()
                return ' | '.join(sorted(formatted_dates))
        return "No date/time data"
    
    def _find_date_column(self, df):
        """Find the most likely date/time column"""
        date_columns = [col for col in df.columns if any(keyword in str(col).lower() for keyword in ['date', 'time', 'timestamp'])]
        
        for col in date_columns:
            sample = df[col].head(5).dropna()
            if len(sample) > 0:
                try:
                    pd.to_datetime(sample, errors='raise')
                    return col
                except:
                    continue
        return None
    
    def _find_duration_column(self, df):
        """Find the most likely duration column"""
        duration_columns = [col for col in df.columns if any(keyword in str(col).lower() for keyword in ['duration', 'call time', 'length'])]
        
        for col in duration_columns:
            sample = df[col].head(5).dropna()
            if len(sample) > 0:
                return col
        return None
    
    def _parse_duration_to_seconds(self, duration):
        """Convert various duration formats to seconds"""
        try:
            # If it's already a number, assume seconds
            if isinstance(duration, (int, float)):
                return float(duration)
            
            duration_str = str(duration).lower().strip()
            
            # Handle HH:MM:SS format
            if ':' in duration_str:
                parts = duration_str.split(':')
                if len(parts) == 3:  # HH:MM:SS
                    hours, minutes, seconds = map(float, parts)
                    return hours * 3600 + minutes * 60 + seconds
                elif len(parts) == 2:  # MM:SS
                    minutes, seconds = map(float, parts)
                    return minutes * 60 + seconds
            
            # Handle text formats like "5 minutes", "1 hour"
            if 'hour' in duration_str or 'hr' in duration_str:
                numbers = re.findall(r'\d+', duration_str)
                hours = float(numbers[0]) if numbers else 1
                return hours * 3600
            elif 'min' in duration_str:
                numbers = re.findall(r'\d+', duration_str)
                minutes = float(numbers[0]) if numbers else 5
                return minutes * 60
            elif 'sec' in duration_str:
                numbers = re.findall(r'\d+', duration_str)
                seconds = float(numbers[0]) if numbers else 30
                return seconds
            
            # Try to extract any number and assume minutes
            numbers = re.findall(r'\d+', duration_str)
            if numbers:
                return float(numbers[0]) * 60  # Assume minutes
            
        except:
            pass
        
        return 0  # Default if cannot parse
    
    def _format_duration(self, total_seconds):
        """Format seconds into HH:MM:SS"""
        if total_seconds == 0:
            return "0:00"
        
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)
        
        if hours > 0:
            return f"{hours}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{minutes}:{seconds:02d}"
    
    def _get_default_date_metrics(self):
        return {
            'avg_gap_between_calls': 0,
            'min_gap_between_calls': 0,
            'max_gap_between_calls': 0,
            'first_call_date': 'Unknown',
            'last_call_date': 'Unknown',
            'total_call_days': 0
        }
    
    def _get_default_time_metrics(self):
        return {
            'total_time_spent_seconds': 0,
            'avg_time_per_call_seconds': 0,
            'total_time_spent': '0:00',
            'avg_time_per_call': '0:00'
        }
    
    def save_all_reports(self, base_output_folder, leads_df, updates_df, call_logs_df, call_analysis_df):
        """Save all reports including the new call analysis"""
        # Create timestamped folder
        timestamp_folder = f"lead_analysis_{self.timestamp}"
        output_folder = os.path.join(base_output_folder, timestamp_folder)
        
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        print(f"💾 Saving all reports to: {output_folder}")
        
        # Save cleaned data files (using DataCleaner's method)
        from data_cleaning import DataCleaner
        cleaner = DataCleaner()
        cleaner.save_cleaned_data(base_output_folder, leads_df, updates_df, call_logs_df)
        
        # Save the call analysis table
        if not call_analysis_df.empty:
            # Select and order the most important columns
            important_columns = [
                'phone', 'name', 'no_of_times_called', 
                'total_time_spent', 'avg_time_per_call',
                'avg_gap_between_calls', 'min_gap_between_calls', 'max_gap_between_calls',
                'first_call_date', 'last_call_date', 'total_call_days',
                'dates_times_called'
            ]
            
            # Only include columns that exist in the dataframe
            available_columns = [col for col in important_columns if col in call_analysis_df.columns]
            final_df = call_analysis_df[available_columns]
            
            final_df.to_csv(os.path.join(output_folder, 'call_analysis_table.csv'), index=False)
            print(f"💾 Saved call analysis table: {len(final_df)} unique phone numbers")
        
        print(f"✅ All reports saved successfully!")
        return output_folder