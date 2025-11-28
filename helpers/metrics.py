# helpers/metrics.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

class LeadMetrics:
    def __init__(self, leads_df, updates_df, call_logs_df):
        self.leads_df = leads_df
        self.updates_df = updates_df
        self.call_logs_df = call_logs_df
        self.summary_df = pd.DataFrame()
        self.employee_summary_df = pd.DataFrame()
        self.lead_detailed_metrics = pd.DataFrame()
    
    def calculate_all_metrics(self):
        """Calculate all metrics for leads and employees"""
        print("📊 Calculating metrics...")
        
        # Calculate per-lead metrics
        self.lead_detailed_metrics = self._calculate_per_lead_metrics()
        
        # Calculate overall summary
        self.summary_df = self._calculate_overall_summary()
        
        # Calculate employee performance
        self.employee_summary_df = self._calculate_employee_metrics()
        
        return self.summary_df, self.employee_summary_df
    
    def _calculate_per_lead_metrics(self):
        """Calculate detailed metrics for each lead"""
        if self.leads_df.empty:
            return pd.DataFrame()
        
        lead_metrics = self.leads_df[['name', 'email', 'phone', 'employee']].copy()
        
        # Initialize metrics columns
        metrics_columns = [
            'total_outgoing_calls', 'total_incoming_calls', 'total_call_duration',
            'first_call_date', 'last_call_date', 'avg_call_gap_days',
            'calls_not_made', 'update_status', 'status_accuracy',
            'follow_up_count', 'contact_attempts', 'conversion_likelihood'
        ]
        
        for col in metrics_columns:
            lead_metrics[col] = 0
        
        # Calculate call metrics for each lead
        for idx, lead in lead_metrics.iterrows():
            phone = lead['phone']
            
            if pd.notna(phone):
                # Get calls for this lead
                lead_calls = self.call_logs_df[self.call_logs_df['phone'] == phone]
                lead_updates = self.updates_df[self.updates_df['phone'] == phone]
                
                # Call metrics
                if not lead_calls.empty:
                    outgoing_calls = lead_calls[lead_calls['call_type'] == 'outgoing']
                    incoming_calls = lead_calls[lead_calls['call_type'] == 'incoming']
                    
                    lead_metrics.at[idx, 'total_outgoing_calls'] = len(outgoing_calls)
                    lead_metrics.at[idx, 'total_incoming_calls'] = len(incoming_calls)
                    lead_metrics.at[idx, 'total_call_duration'] = lead_calls['duration'].sum()
                    
                    # Call dates
                    if not outgoing_calls.empty:
                        lead_metrics.at[idx, 'first_call_date'] = outgoing_calls['timestamp'].min()
                        lead_metrics.at[idx, 'last_call_date'] = outgoing_calls['timestamp'].max()
                        
                        # Calculate average gap between calls
                        if len(outgoing_calls) > 1:
                            call_dates = outgoing_calls['timestamp'].sort_values()
                            gaps = (call_dates.diff().dt.total_seconds() / 86400).dropna()  # Convert to days
                            lead_metrics.at[idx, 'avg_call_gap_days'] = gaps.mean()
                
                # Update metrics
                if not lead_updates.empty:
                    latest_update = lead_updates.sort_values('timestamp').iloc[-1]
                    lead_metrics.at[idx, 'update_status'] = latest_update['update_type']
                    
                    # Count follow-ups
                    followup_updates = lead_updates[lead_updates['update_type'].str.contains('followup', na=False)]
                    lead_metrics.at[idx, 'follow_up_count'] = len(followup_updates)
                    
                    # Contact attempts (calls + followups)
                    lead_metrics.at[idx, 'contact_attempts'] = len(lead_calls) + len(followup_updates)
                
                # Status accuracy (check if updates match call reality)
                lead_metrics.at[idx, 'status_accuracy'] = self._calculate_status_accuracy(lead_calls, lead_updates)
                
                # Calls not made (if employee said they called but no call records)
                lead_metrics.at[idx, 'calls_not_made'] = self._calculate_calls_not_made(lead_calls, lead_updates)
                
                # Conversion likelihood score
                lead_metrics.at[idx, 'conversion_likelihood'] = self._calculate_conversion_score(lead_calls, lead_updates)
        
        return lead_metrics
    
    def _calculate_status_accuracy(self, lead_calls, lead_updates):
        """Calculate how accurate the status updates are compared to actual calls"""
        if lead_updates.empty or lead_calls.empty:
            return 0.0
        
        accuracy_score = 0
        total_checks = 0
        
        for _, update in lead_updates.iterrows():
            update_time = update['timestamp']
            update_type = update['update_type']
            
            # Check if there's a call around the update time
            time_window_start = update_time - timedelta(hours=2)
            time_window_end = update_time + timedelta(hours=2)
            
            calls_in_window = lead_calls[
                (lead_calls['timestamp'] >= time_window_start) & 
                (lead_calls['timestamp'] <= time_window_end)
            ]
            
            if not calls_in_window.empty:
                accuracy_score += 1
            
            total_checks += 1
        
        return accuracy_score / total_checks if total_checks > 0 else 0.0
    
    def _calculate_calls_not_made(self, lead_calls, lead_updates):
        """Count updates claiming calls were made but no call records exist"""
        if lead_updates.empty:
            return 0
        
        calls_not_made = 0
        
        for _, update in lead_updates.iterrows():
            update_text = str(update['update_text']).lower()
            update_time = update['timestamp']
            
            # Check if update mentions a call
            call_keywords = ['called', 'call', 'dialed', 'phoned', 'rang']
            if any(keyword in update_text for keyword in call_keywords):
                # Look for calls around this time
                time_window_start = update_time - timedelta(hours=4)
                time_window_end = update_time + timedelta(hours=4)
                
                calls_in_window = lead_calls[
                    (lead_calls['timestamp'] >= time_window_start) & 
                    (lead_calls['timestamp'] <= time_window_end)
                ]
                
                if calls_in_window.empty:
                    calls_not_made += 1
        
        return calls_not_made
    
    def _calculate_conversion_score(self, lead_calls, lead_updates):
        """Calculate conversion likelihood score (0-100)"""
        score = 50  # Base score
        
        # Positive factors
        if not lead_calls.empty:
            score += min(len(lead_calls) * 2, 20)  # More calls = better
            score += min(lead_calls['duration'].sum() / 60, 10)  # Longer calls = better
        
        if not lead_updates.empty:
            positive_updates = lead_updates[lead_updates['update_type'].isin([
                'interested', 'meeting_scheduled', 'demo_scheduled', 'callback'
            ])]
            score += len(positive_updates) * 5
        
        # Negative factors
        if not lead_updates.empty:
            negative_updates = lead_updates[lead_updates['update_type'].isin([
                'not_interested', 'wrong_number', 'busy'
            ])]
            score -= len(negative_updates) * 10
        
        return max(0, min(100, score))
    
    def _calculate_overall_summary(self):
        """Calculate overall summary metrics"""
        if self.lead_detailed_metrics.empty:
            return pd.DataFrame()
        
        summary_data = {
            'total_leads': len(self.leads_df),
            'contacted_leads': len(self.lead_detailed_metrics[self.lead_detailed_metrics['total_outgoing_calls'] > 0]),
            'converted_leads': len(self.lead_detailed_metrics[self.lead_detailed_metrics['update_status'].isin([
                'interested', 'meeting_scheduled', 'demo_scheduled'
            ])]),
            'never_contacted_leads': len(self.lead_detailed_metrics[self.lead_detailed_metrics['total_outgoing_calls'] == 0]),
            'poor_followup_leads': len(self.lead_detailed_metrics[
                (self.lead_detailed_metrics['total_outgoing_calls'] > 0) & 
                (self.lead_detailed_metrics['follow_up_count'] == 0)
            ]),
            'total_calls': self.call_logs_df['call_type'].count(),
            'outgoing_calls': len(self.call_logs_df[self.call_logs_df['call_type'] == 'outgoing']),
            'incoming_calls': len(self.call_logs_df[self.call_logs_df['call_type'] == 'incoming']),
            'total_call_duration': self.call_logs_df['duration'].sum(),
            'avg_calls_per_lead': self.lead_detailed_metrics['total_outgoing_calls'].mean(),
            'avg_call_duration': self.call_logs_df['duration'].mean(),
            'followup_rate': self.lead_detailed_metrics['follow_up_count'].mean(),
            'status_accuracy_avg': self.lead_detailed_metrics['status_accuracy'].mean(),
            'calls_not_made_total': self.lead_detailed_metrics['calls_not_made'].sum()
        }
        
        # Calculate rates
        summary_data['contact_rate'] = summary_data['contacted_leads'] / summary_data['total_leads'] if summary_data['total_leads'] > 0 else 0
        summary_data['conversion_rate'] = summary_data['converted_leads'] / summary_data['total_leads'] if summary_data['total_leads'] > 0 else 0
        
        return pd.DataFrame([summary_data])
    
    def _calculate_employee_metrics(self):
        """Calculate performance metrics for each employee"""
        if self.leads_df.empty:
            return pd.DataFrame()
        
        employees = self.leads_df['employee'].unique()
        employee_metrics = []
        
        for employee in employees:
            emp_leads = self.leads_df[self.leads_df['employee'] == employee]
            emp_calls = self.call_logs_df[self.call_logs_df['employee'] == employee]
            emp_updates = self.updates_df[self.updates_df['employee'] == employee]
            
            emp_lead_metrics = self.lead_detailed_metrics[self.lead_detailed_metrics['employee'] == employee]
            
            if not emp_leads.empty:
                emp_data = {
                    'employee': employee,
                    'total_leads': len(emp_leads),
                    'contacted_leads': len(emp_lead_metrics[emp_lead_metrics['total_outgoing_calls'] > 0]),
                    'converted_leads': len(emp_lead_metrics[emp_lead_metrics['update_status'].isin([
                        'interested', 'meeting_scheduled', 'demo_scheduled'
                    ])]),
                    'total_calls': len(emp_calls),
                    'outgoing_calls': len(emp_calls[emp_calls['call_type'] == 'outgoing']),
                    'avg_calls_per_lead': emp_lead_metrics['total_outgoing_calls'].mean(),
                    'avg_call_duration': emp_calls['duration'].mean(),
                    'follow_up_count': emp_lead_metrics['follow_up_count'].sum(),
                    'status_accuracy': emp_lead_metrics['status_accuracy'].mean(),
                    'calls_not_made': emp_lead_metrics['calls_not_made'].sum()
                }
                
                # Calculate rates
                emp_data['contact_rate'] = emp_data['contacted_leads'] / emp_data['total_leads'] if emp_data['total_leads'] > 0 else 0
                emp_data['conversion_rate'] = emp_data['converted_leads'] / emp_data['total_leads'] if emp_data['total_leads'] > 0 else 0
                emp_data['followup_rate'] = emp_data['follow_up_count'] / emp_data['contacted_leads'] if emp_data['contacted_leads'] > 0 else 0
                
                employee_metrics.append(emp_data)
        
        return pd.DataFrame(employee_metrics)
    
    def get_lead_categories(self):
        """Categorize leads based on their status and activity"""
        if self.lead_detailed_metrics.empty:
            return {}
        
        categories = {
            'hot_leads': self.lead_detailed_metrics[
                self.lead_detailed_metrics['conversion_likelihood'] >= 70
            ],
            'warm_leads': self.lead_detailed_metrics[
                (self.lead_detailed_metrics['conversion_likelihood'] >= 40) & 
                (self.lead_detailed_metrics['conversion_likelihood'] < 70)
            ],
            'cold_leads': self.lead_detailed_metrics[
                self.lead_detailed_metrics['conversion_likelihood'] < 40
            ],
            'never_contacted': self.lead_detailed_metrics[
                self.lead_detailed_metrics['total_outgoing_calls'] == 0
            ],
            'needs_followup': self.lead_detailed_metrics[
                (self.lead_detailed_metrics['total_outgoing_calls'] > 0) & 
                (self.lead_detailed_metrics['follow_up_count'] == 0) & 
                (self.lead_detailed_metrics['conversion_likelihood'] >= 40)
            ]
        }
        
        return categories
    
    def save_metrics(self, output_folder):
        """Save all metrics to files"""
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        if not self.summary_df.empty:
            self.summary_df.to_csv(os.path.join(output_folder, 'summary_metrics.csv'), index=False)
        
        if not self.employee_summary_df.empty:
            self.employee_summary_df.to_csv(os.path.join(output_folder, 'employee_metrics.csv'), index=False)
        
        if not self.lead_detailed_metrics.empty:
            self.lead_detailed_metrics.to_csv(os.path.join(output_folder, 'lead_detailed_metrics.csv'), index=False)
        
        # Save lead categories
        categories = self.get_lead_categories()
        for category_name, category_df in categories.items():
            if not category_df.empty:
                category_df.to_csv(os.path.join(output_folder, f'leads_{category_name}.csv'), index=False)
        
        print(f"💾 Metrics saved to: {output_folder}")