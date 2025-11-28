# helpers/data_cleaning.py
import pandas as pd
import os
import re
from datetime import datetime

class DataCleaner:
    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    def find_files(self, base_folder):
        """Find all CSV/Excel files in folder structure"""
        all_files = []
        
        for root, dirs, files in os.walk(base_folder):
            for file in files:
                if file.endswith(('.csv', '.xlsx', '.xls')):
                    full_path = os.path.join(root, file)
                    all_files.append(full_path)
        
        print(f"🔍 Found {len(all_files)} files")
        return all_files
    
    def categorize_files(self, file_list):
        """Better file categorization based on filename patterns"""
        leads_files = []
        updates_files = []
        call_logs_files = []
        
        for file in file_list:
            filename_lower = os.path.basename(file).lower()
            
            # More specific categorization
            if any(keyword in filename_lower for keyword in ['lead', 'prospect', 'contact', 'customer']):
                if any(keyword in filename_lower for keyword in ['update', 'updated']):
                    updates_files.append(file)
                else:
                    leads_files.append(file)
            
            elif any(keyword in filename_lower for keyword in ['update', 'followup', 'status', 'progress']):
                updates_files.append(file)
            
            elif any(keyword in filename_lower for keyword in ['call', 'log', 'dial', 'report', 'communication']):
                call_logs_files.append(file)
            
            else:
                # Default to leads if unsure
                leads_files.append(file)
        
        print(f"📂 Categorized: {len(leads_files)} leads, {len(updates_files)} updates, {len(call_logs_files)} call logs")
        return leads_files, updates_files, call_logs_files
    
    def clean_phone_number(self, phone):
        """Standardize phone number format"""
        if pd.isna(phone) or phone in ['', 'nan', 'None', 'null']:
            return None
        
        phone_str = str(phone).strip()
        
        # Remove all non-digit characters
        cleaned = re.sub(r'[^\d]', '', phone_str)
        
        # Handle Sri Lankan numbers
        if cleaned.startswith('94'):
            cleaned = '0' + cleaned[2:]
        elif cleaned.startswith('+94'):
            cleaned = '0' + cleaned[3:]
        elif len(cleaned) == 9 and not cleaned.startswith('0'):
            cleaned = '0' + cleaned
        
        # Ensure valid length (10 digits for Sri Lanka)
        if len(cleaned) == 10 and cleaned.startswith('0'):
            return cleaned
        else:
            return None
    
    def clean_email(self, email):
        """Standardize email format"""
        if pd.isna(email) or email in ['', 'nan', 'None', 'null']:
            return None
        
        email_str = str(email).strip().lower()
        
        # Basic email validation
        if '@' in email_str and '.' in email_str and len(email_str) > 5:
            return email_str
        else:
            return None
    
    def extract_contact_info(self, df):
        """Extract name, email, phone, city from dataframe"""
        result = {'name': None, 'email': None, 'phone': None, 'city': None}
        
        # Convert all column names to lowercase for matching
        df.columns = [str(col).lower().strip() for col in df.columns]
        
        # Find name column
        name_patterns = ['name', 'fullname', 'contact', 'customer', 'person']
        for col in df.columns:
            if any(pattern in col for pattern in name_patterns):
                result['name'] = df[col]
                break
        
        # Find email column
        email_patterns = ['email', 'mail']
        for col in df.columns:
            if any(pattern in col for pattern in email_patterns):
                result['email'] = df[col]
                break
        
        # Find phone column
        phone_patterns = ['phone', 'mobile', 'number', 'contact', 'phonenumber']
        for col in df.columns:
            if any(pattern in col for pattern in phone_patterns):
                result['phone'] = df[col]
                break
        
        # Find city column
        city_patterns = ['city', 'location', 'area', 'town']
        for col in df.columns:
            if any(pattern in col for pattern in city_patterns):
                result['city'] = df[col]
                break
        
        return result
    
    def extract_updates_info(self, df):
        """Extract all update columns and combine them"""
        df.columns = [str(col).lower().strip() for col in df.columns]
        
        # Get contact info (including city)
        contact_info = self.extract_contact_info(df)
        
        # Find ALL update-related columns (not contact info columns)
        update_columns = []
        contact_cols = ['name', 'email', 'phone', 'mobile', 'contact', 'city', 'location', 'area', 'town']
        
        for col in df.columns:
            is_contact_col = any(contact in col for contact in contact_cols)
            is_update_col = any(keyword in col for keyword in [
                'update', 'call', 'followup', 'follow', '1st', '2nd', '3rd', 
                'first', 'second', 'third', 'status', 'note', 'remark', 'comment'
            ])
            
            if not is_contact_col and is_update_col:
                update_columns.append(col)
        
        # If no specific update columns found, use all non-contact columns
        if not update_columns:
            update_columns = [col for col in df.columns if not any(contact in col for contact in contact_cols)]
        
        return contact_info, update_columns
    
    def merge_leads_files(self, leads_files):
        """Merge and clean all leads files - keep name, email, phone, city"""
        all_leads = []
        
        for file in leads_files:
            try:
                # Read file
                if file.endswith('.csv'):
                    df = pd.read_csv(file)
                else:
                    df = pd.read_excel(file)
                
                print(f"📖 Reading leads: {os.path.basename(file)}")
                
                # Extract contact info (name, email, phone, city)
                contact_info = self.extract_contact_info(df)
                
                # Create standardized dataframe with required columns + city
                lead_data = {
                    'name': contact_info['name'],
                    'email': contact_info['email'] if contact_info['email'] is not None else [None] * len(df),
                    'phone': contact_info['phone'],
                    'original_file': os.path.basename(file),
                    'employee': self._extract_employee_name(file)
                }
                
                # Add city if it exists
                if contact_info['city'] is not None:
                    lead_data['city'] = contact_info['city']
                
                standardized_df = pd.DataFrame(lead_data)
                all_leads.append(standardized_df)
                
                print(f"✅ Processed leads: {os.path.basename(file)}")
                if contact_info['city'] is not None:
                    print(f"   📍 City column found and included")
                
            except Exception as e:
                print(f"❌ Error processing {file}: {e}")
        
        if all_leads:
            merged_leads = pd.concat(all_leads, ignore_index=True)
            
            # Clean data
            merged_leads['phone'] = merged_leads['phone'].apply(self.clean_phone_number)
            merged_leads['email'] = merged_leads['email'].apply(self.clean_email)
            merged_leads['name'] = merged_leads['name'].astype(str).str.title().str.strip()
            
            # Clean city if it exists
            if 'city' in merged_leads.columns:
                merged_leads['city'] = merged_leads['city'].astype(str).str.title().str.strip()
            
            # Remove duplicates based on phone + email
            before_dedup = len(merged_leads)
            merged_leads = merged_leads.drop_duplicates(subset=['phone', 'email'], keep='first')
            after_dedup = len(merged_leads)
            
            print(f"🎯 Leads: {after_dedup} records (removed {before_dedup - after_dedup} duplicates)")
            if 'city' in merged_leads.columns:
                print(f"📍 City data included: {merged_leads['city'].notna().sum()} records have city info")
            
            return merged_leads
        else:
            return pd.DataFrame()
    
    def merge_updates_files(self, updates_files):
        """Merge and clean all updates files - handle multiple update columns, keep city"""
        all_updates = []
        
        for file in updates_files:
            try:
                # Read file
                if file.endswith('.csv'):
                    df = pd.read_csv(file)
                else:
                    df = pd.read_excel(file)
                
                print(f"📖 Reading updates: {os.path.basename(file)}")
                
                # Extract contact info and update columns (including city)
                contact_info, update_columns = self.extract_updates_info(df)
                
                print(f"   Found update columns: {update_columns}")
                
                # Process each row to combine all update columns
                processed_rows = []
                for idx in range(len(df)):
                    # Get contact info for this row (including city)
                    row_contact = {}
                    for key, value in contact_info.items():
                        if value is not None:
                            row_contact[key] = value.iloc[idx] if hasattr(value, 'iloc') else value[idx]
                        else:
                            row_contact[key] = None
                    
                    # Combine all update columns for this row
                    update_texts = []
                    for col in update_columns:
                        update_val = df.iloc[idx][col]
                        if pd.notna(update_val) and str(update_val).strip() not in ['', 'nan', 'None']:
                            update_texts.append(f"{col}: {str(update_val).strip()}")
                    
                    # Create combined update text
                    combined_update = " | ".join(update_texts) if update_texts else "No updates"
                    
                    # Create row data (including city)
                    row_data = {
                        'name': row_contact['name'],
                        'email': row_contact['email'],
                        'phone': row_contact['phone'],
                        'update_text': combined_update,
                        'original_file': os.path.basename(file),
                        'employee': self._extract_employee_name(file),
                        'timestamp': datetime.now()
                    }
                    
                    # Add city if it exists
                    if row_contact['city'] is not None:
                        row_data['city'] = row_contact['city']
                    
                    processed_rows.append(row_data)
                
                standardized_df = pd.DataFrame(processed_rows)
                all_updates.append(standardized_df)
                
                print(f"✅ Processed updates: {os.path.basename(file)}")
                if contact_info['city'] is not None:
                    print(f"   📍 City column found and included")
                
            except Exception as e:
                print(f"❌ Error processing {file}: {e}")
        
        if all_updates:
            merged_updates = pd.concat(all_updates, ignore_index=True)
            
            # Clean data
            merged_updates['phone'] = merged_updates['phone'].apply(self.clean_phone_number)
            merged_updates['email'] = merged_updates['email'].apply(self.clean_email)
            merged_updates['name'] = merged_updates['name'].astype(str).str.title().str.strip()
            
            # Clean city if it exists
            if 'city' in merged_updates.columns:
                merged_updates['city'] = merged_updates['city'].astype(str).str.title().str.strip()
            
            # Remove duplicates
            before_dedup = len(merged_updates)
            merged_updates = merged_updates.drop_duplicates(subset=['name', 'phone', 'update_text'], keep='first')
            after_dedup = len(merged_updates)
            
            print(f"🎯 Updates: {after_dedup} records (removed {before_dedup - after_dedup} duplicates)")
            if 'city' in merged_updates.columns:
                print(f"📍 City data included: {merged_updates['city'].notna().sum()} records have city info")
            
            return merged_updates
        else:
            return pd.DataFrame()
    
    def merge_call_logs(self, call_logs_files):
        """Merge call log files WITH phone number standardization"""
        all_call_logs = []
        
        for file in call_logs_files:
            try:
                # Read file
                if file.endswith('.csv'):
                    df = pd.read_csv(file)
                else:
                    df = pd.read_excel(file)
                
                print(f"📖 Reading call logs: {os.path.basename(file)}")
                
                # Extract contact info
                contact_info = self.extract_contact_info(df)
                
                # Create standardized dataframe
                call_data = {
                    'name': contact_info['name'],
                    'phone': contact_info['phone'],
                    'original_file': os.path.basename(file),
                    'employee': self._extract_employee_name(file)
                }
                
                # Add all other columns from the original file
                for col in df.columns:
                    col_lower = str(col).lower()
                    if not any(keyword in col_lower for keyword in ['name', 'phone', 'mobile', 'number']):
                        call_data[col] = df[col]
                
                standardized_df = pd.DataFrame(call_data)
                all_call_logs.append(standardized_df)
                
                print(f"✅ Processed call logs: {os.path.basename(file)}")
                
            except Exception as e:
                print(f"❌ Error processing {file}: {e}")
        
        if all_call_logs:
            # Merge all call logs
            merged_calls = pd.concat(all_call_logs, ignore_index=True)
            
            # Clean phone numbers
            merged_calls['phone_cleaned'] = merged_calls['phone'].apply(self.clean_phone_number)
            
            # Remove rows without valid phone numbers
            before_clean = len(merged_calls)
            merged_calls = merged_calls[merged_calls['phone_cleaned'].notna()]
            after_clean = len(merged_calls)
            
            print(f"🎯 Call logs: {after_clean} records with valid phone numbers (removed {before_clean - after_clean} invalid)")
            
            return merged_calls
        else:
            return pd.DataFrame()
    
    def _extract_employee_name(self, filepath):
        """Extract employee name from immediate subfolder name"""
        path_parts = filepath.split(os.sep)
        
        # Common folder names to ignore
        common_folders = ['data', 'leads', 'updates', 'call_logs', 'calls', 'reports', ''] 
        
        # Look for the first non-common folder name (should be employee name)
        for part in reversed(path_parts[:-1]):  # Exclude filename, go backwards
            if (part not in common_folders and 
                not part.startswith('.') and 
                len(part) > 2):
                return part.title()
        
        return 'Unknown'
    
    def process_all_data(self, base_folder):
        """Main method to process all data"""
        print("=" * 50)
        print("🔄 STARTING DATA PROCESSING")
        print("=" * 50)
        
        all_files = self.find_files(base_folder)
        
        if not all_files:
            print("❌ No files found in the selected folder!")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
        leads_files, updates_files, call_logs_files = self.categorize_files(all_files)
        
        print(f"\n📊 Processing files...")
        print(f"   Leads: {len(leads_files)} files")
        print(f"   Updates: {len(updates_files)} files")
        print(f"   Call Logs: {len(call_logs_files)} files")
        
        # Process each category
        leads_df = self.merge_leads_files(leads_files)
        updates_df = self.merge_updates_files(updates_files)
        call_logs_df = self.merge_call_logs(call_logs_files)
        
        print("\n" + "=" * 50)
        print("✅ DATA PROCESSING COMPLETE")
        print("=" * 50)
        
        return leads_df, updates_df, call_logs_df
    
    def save_cleaned_data(self, base_output_folder, leads_df, updates_df, call_logs_df):
        """Save cleaned data to timestamped folder"""
        # Create timestamped folder
        timestamp_folder = f"cleaned_data_{self.timestamp}"
        output_folder = os.path.join(base_output_folder, timestamp_folder)
        
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        # Save cleaned files
        if not leads_df.empty:
            leads_df.to_csv(os.path.join(output_folder, 'cleaned_leads.csv'), index=False)
            print(f"💾 Saved leads: {len(leads_df)} records")
        
        if not updates_df.empty:
            updates_df.to_csv(os.path.join(output_folder, 'cleaned_updates.csv'), index=False)
            print(f"💾 Saved updates: {len(updates_df)} records")
        
        if not call_logs_df.empty:
            call_logs_df.to_csv(os.path.join(output_folder, 'cleaned_call_logs.csv'), index=False)
            print(f"💾 Saved call logs: {len(call_logs_df)} records")
        
        # Create and save overall performance summary
        self._create_overall_performance(output_folder, leads_df, updates_df, call_logs_df)
        
        print(f"📁 All files saved to: {output_folder}")
        return output_folder
    
    def _create_overall_performance(self, output_folder, leads_df, updates_df, call_logs_df):
        """Create simple overall performance summary"""
        performance_data = []
        
        # Basic counts
        performance_data.append({'Metric': 'Total Leads', 'Value': len(leads_df)})
        performance_data.append({'Metric': 'Total Updates', 'Value': len(updates_df)})
        performance_data.append({'Metric': 'Total Call Logs', 'Value': len(call_logs_df)})
        
        # Employee counts
        if not leads_df.empty:
            employees = leads_df['employee'].nunique()
            performance_data.append({'Metric': 'Total Employees', 'Value': employees})
        
        # Contact rate (simplified)
        if not leads_df.empty and not updates_df.empty:
            # Simple contact rate calculation
            contacted_leads = len(updates_df['phone'].dropna().unique())
            total_leads = len(leads_df)
            contact_rate = (contacted_leads / total_leads * 100) if total_leads > 0 else 0
            performance_data.append({'Metric': 'Contact Rate', 'Value': f"{contact_rate:.1f}%"})
        
        # Save performance summary
        performance_df = pd.DataFrame(performance_data)
        performance_df.to_csv(os.path.join(output_folder, 'overall_performance.csv'), index=False)
        print(f"💾 Saved overall performance: {len(performance_df)} metrics")