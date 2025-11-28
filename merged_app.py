import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os
import glob
import re

class UnifiedProcessor:
    def __init__(self):
        self.call_logs_df = None
        self.leads_df = None
        self.processed_call_logs = None
        self.processed_leads = None
        
    # ===== CALL LOGS PROCESSING METHODS =====
    
    def standardize_phone_number(self, phone_str):
        """Standardize phone number to start with 94 country code"""
        if pd.isna(phone_str) or phone_str == '':
            return phone_str
            
        # Convert to string and remove all non-digit characters using regex
        clean_number = re.sub(r'\D', '', str(phone_str))
        
        # Remove trailing zeros
        clean_number = clean_number.rstrip('0')
        
        # If number is empty after cleaning, return as is
        if not clean_number:
            return phone_str
            
        # Standardize to 94 country code
        if clean_number.startswith('94'):
            # Already starts with 94, ensure proper length
            if len(clean_number) >= 9:  # 94 + 7 digits minimum
                return clean_number
            else:
                return phone_str  # Return original if too short
        elif len(clean_number) == 9:
            # 9-digit number without country code, add 94
            return '94' + clean_number
        elif len(clean_number) == 10 and clean_number.startswith('0'):
            # 10-digit number starting with 0, replace 0 with 94
            return '94' + clean_number[1:]
        elif len(clean_number) == 7:
            # 7-digit local number, add 94
            return '94' + clean_number
        else:
            # For other formats, try to make it start with 94
            if len(clean_number) > 9:
                # If longer than 9 digits, assume it includes country code but not 94
                # Try to extract last 9 digits and add 94
                last_9_digits = clean_number[-9:]
                return '94' + last_9_digits
            else:
                return '94' + clean_number  # Default: add 94 prefix

    def parse_duration(self, duration_str):
        """Convert duration string to total seconds"""
        try:
            if pd.isna(duration_str) or duration_str == '':
                return 0
            
            # Handle different duration formats
            if 'h' in duration_str and 'm' in duration_str and 's' in duration_str:
                # Format: "00h 00m 00s"
                parts = duration_str.split()
                hours = int(parts[0].replace('h', '')) if len(parts) > 0 else 0
                minutes = int(parts[1].replace('m', '')) if len(parts) > 1 else 0
                seconds = int(parts[2].replace('s', '')) if len(parts) > 2 else 0
                return hours * 3600 + minutes * 60 + seconds
            else:
                return 0
        except:
            return 0

    def calculate_time_gaps(self, dates):
        """Calculate average gap between consecutive calls"""
        if len(dates) <= 1:
            return 0
        
        dates_sorted = sorted(dates)
        gaps = []
        
        for i in range(1, len(dates_sorted)):
            gap = (dates_sorted[i] - dates_sorted[i-1]).total_seconds() / 3600  # Gap in hours
            gaps.append(gap)
        
        return np.mean(gaps) if gaps else 0

    def process_call_logs(self, folder_path):
        """Process call log files from folder"""
        try:
            # Find all CSV files in the folder
            csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
            
            if not csv_files:
                return False, "No CSV files found for call logs"
            
            # List to store all dataframes
            all_dfs = []
            
            for file_path in csv_files:
                try:
                    # Read each CSV file
                    df = pd.read_csv(file_path)
                    print(f"Loaded call log file: {os.path.basename(file_path)} with {len(df)} records")
                    all_dfs.append(df)
                except Exception as e:
                    print(f"Error loading file {os.path.basename(file_path)}: {str(e)}")
                    continue
            
            if not all_dfs:
                return False, "No valid call log files could be loaded"
            
            # Merge all dataframes
            self.call_logs_df = pd.concat(all_dfs, ignore_index=True)
            print(f"Total call records after merging: {len(self.call_logs_df)}")
            
            # Basic cleaning
            self.call_logs_df = self.call_logs_df.dropna(subset=['To Number'])  # Remove rows with no To Number
            self.call_logs_df = self.call_logs_df[self.call_logs_df['To Number'].notna()]
            
            # Convert Date Time column to datetime
            self.call_logs_df['Date Time'] = pd.to_datetime(self.call_logs_df['Date Time'], errors='coerce')
            
            # Clean and standardize phone numbers
            self.call_logs_df['To Number'] = self.call_logs_df['To Number'].apply(self.standardize_phone_number)
            
            # Remove scientific notation numbers and invalid phone numbers
            self.call_logs_df = self.call_logs_df[~self.call_logs_df['To Number'].str.contains('E', na=False)]
            self.call_logs_df = self.call_logs_df[self.call_logs_df['To Number'].str.len() >= 9]  # Minimum 9 digits (94 + 7)
            
            # Convert Duration to seconds
            self.call_logs_df['Duration_Seconds'] = self.call_logs_df['Duration'].apply(self.parse_duration)
            
            # Remove duplicates
            initial_count = len(self.call_logs_df)
            self.call_logs_df = self.call_logs_df.drop_duplicates()
            duplicates_removed = initial_count - len(self.call_logs_df)
            print(f"Removed {duplicates_removed} exact duplicate call records")
            
            # Process the call data
            records = []
            
            # Group by phone number
            for phone_number, group in self.call_logs_df.groupby('To Number'):
                total_calls = len(group)
                total_duration = group['Duration_Seconds'].sum()
                call_dates = group['Date Time'].tolist()
                call_times = group['Time'].tolist()
                
                # Calculate metrics
                avg_gap_hours = self.calculate_time_gaps(call_dates)
                first_call = min(call_dates) if call_dates else None
                last_call = max(call_dates) if call_dates else None
                
                # Get name (most frequent one)
                names = group['Name'].value_counts()
                most_common_name = names.index[0] if len(names) > 0 else 'Unknown'
                
                # Format date-time called column - ALL dates and times
                date_time_called = []
                for date, time in zip(call_dates, call_times):
                    if pd.notna(date):
                        date_str = date.strftime('%Y-%m-%d')
                        date_time_called.append(f"{date_str} {time}")
                    else:
                        date_time_called.append(f"Unknown Date {time}")
                
                # Join all dates and times with comma separation
                all_dates_times = ', '.join(date_time_called)
                
                records.append({
                    'Phone Number': phone_number,
                    'Name': most_common_name,
                    'Total Calls': total_calls,
                    'Total Duration (seconds)': total_duration,
                    'Total Duration (HH:MM:SS)': str(timedelta(seconds=int(total_duration))),
                    'First Call Date': first_call,
                    'Last Call Date': last_call,
                    'Avg Gap (hours)': round(avg_gap_hours, 2),
                    'All Dates and Times': all_dates_times,  # All dates and times in one column
                    'Call Dates': call_dates,
                    'Call Times': call_times
                })
            
            self.processed_call_logs = pd.DataFrame(records)
            return True, f"Processed {len(csv_files)} call log files, {len(self.processed_call_logs)} unique numbers, removed {duplicates_removed} duplicates"
            
        except Exception as e:
            return False, f"Error processing call logs: {str(e)}"

    # ===== LEADS PROCESSING METHODS =====

    def identify_column_type(self, column_name):
        """Identify what type of column this is based on name patterns"""
        col_lower = str(column_name).lower()
        
        # Name patterns
        name_patterns = ['name', 'full name', 'fullname', 'first name', 'contact name', 'person']
        if any(pattern in col_lower for pattern in name_patterns):
            return 'Name'
        
        # Phone patterns
        phone_patterns = ['phone', 'number', 'phone number', 'contact', 'tel', 'telephone', 'mobile']
        if any(pattern in col_lower for pattern in phone_patterns):
            return 'Phone'
        
        # Email patterns
        email_patterns = ['email', 'e-mail', 'mail', 'gmail']
        if any(pattern in col_lower for pattern in email_patterns):
            return 'Email'
        
        # City patterns
        city_patterns = ['city', 'town', 'location', 'area', 'district']
        if any(pattern in col_lower for pattern in city_patterns):
            return 'City'
        
        # Update patterns
        update_patterns = [
            'update', 'followup', 'follow up', 'status', 'remark', 'comment', 'note',
            'call', 'follow', 'weekend'
        ]
        if any(pattern in col_lower for pattern in update_patterns):
            return 'Update'
        
        return 'Other'

    def process_leads(self, folder_path):
        """Process leads files from folder"""
        try:
            # Find all CSV and Excel files in the folder
            csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
            excel_files = glob.glob(os.path.join(folder_path, "*.xlsx"))
            excel_files.extend(glob.glob(os.path.join(folder_path, "*.xls")))
            
            all_files = csv_files + excel_files
            
            if not all_files:
                return False, "No CSV or Excel files found for leads"
            
            # Filter files that have "leads" in the filename (case insensitive)
            leads_files = []
            for file_path in all_files:
                filename = os.path.basename(file_path).lower()
                if 'leads' in filename:
                    leads_files.append(file_path)
                    print(f"Found leads file: {os.path.basename(file_path)}")
                else:
                    print(f"Ignoring non-leads file: {os.path.basename(file_path)}")
            
            if not leads_files:
                return False, f"No files with 'leads' in filename found. Found {len(all_files)} files but none contain 'leads' in their names."
            
            # List to store all dataframes
            all_dfs = []
            
            for file_path in leads_files:
                try:
                    # Determine file type and read accordingly
                    file_ext = os.path.splitext(file_path)[1].lower()
                    
                    if file_ext == '.csv':
                        df = pd.read_csv(file_path)
                    elif file_ext in ['.xlsx', '.xls']:
                        # Read first sheet of Excel file
                        df = pd.read_excel(file_path, sheet_name=0)
                    else:
                        print(f"Unsupported file format: {file_path}")
                        continue
                    
                    print(f"Loaded leads file: {os.path.basename(file_path)} with {len(df)} records and {len(df.columns)} columns")
                    all_dfs.append(df)
                    
                except Exception as e:
                    print(f"Error loading file {os.path.basename(file_path)}: {str(e)}")
                    continue
            
            if not all_dfs:
                return False, "No valid leads files could be loaded"
            
            # Merge all dataframes
            self.leads_df = pd.concat(all_dfs, ignore_index=True)
            print(f"Total leads records after merging: {len(self.leads_df)}")
            print(f"Original columns: {list(self.leads_df.columns)}")
            
            # STEP 1: Identify and categorize all columns
            column_categories = {}
            for col in self.leads_df.columns:
                col_type = self.identify_column_type(col)
                if col_type not in column_categories:
                    column_categories[col_type] = []
                column_categories[col_type].append(col)
            
            print(f"Column categories: {column_categories}")
            
            # STEP 2: Create standard columns by combining similar ones
            standard_columns = {}
            
            # Process each category
            for col_type, columns in column_categories.items():
                if col_type in ['Name', 'Phone', 'Email', 'City']:
                    # For main columns, combine all similar columns
                    if columns:
                        # Create combined column (take first non-null value from any of the source columns)
                        combined_values = []
                        for idx in range(len(self.leads_df)):
                            value = ''
                            for col in columns:
                                if pd.notna(self.leads_df.iloc[idx][col]) and str(self.leads_df.iloc[idx][col]).strip() != '':
                                    value = str(self.leads_df.iloc[idx][col]).strip()
                                    break
                            combined_values.append(value)
                        
                        standard_columns[col_type] = combined_values
                elif col_type == 'Update':
                    # For update columns, we'll merge them later
                    standard_columns['Update_Columns'] = columns
                else:
                    # Keep other columns as they are
                    for col in columns:
                        standard_columns[col] = self.leads_df[col]
            
            # STEP 3: Create new dataframe with standard columns
            new_data = {}
            
            # Add standard columns
            for col_type in ['Name', 'Phone', 'Email', 'City']:
                if col_type in standard_columns:
                    new_data[col_type] = standard_columns[col_type]
                else:
                    new_data[col_type] = [''] * len(self.leads_df)
            
            # Add update columns (we'll merge them later)
            if 'Update_Columns' in standard_columns:
                update_cols = standard_columns['Update_Columns']
                for col in update_cols:
                    new_data[col] = self.leads_df[col]
            
            # Add other columns
            for col_name, values in standard_columns.items():
                if col_name not in ['Name', 'Phone', 'Email', 'City', 'Update_Columns']:
                    new_data[col_name] = values
            
            self.leads_df = pd.DataFrame(new_data)
            
            print(f"Columns after standardization: {list(self.leads_df.columns)}")
            
            # STEP 4: Standardize phone numbers
            if 'Phone' in self.leads_df.columns:
                self.leads_df['Phone'] = self.leads_df['Phone'].apply(self.standardize_phone_number)
            
            # STEP 5: Merge update columns
            update_columns = [col for col in self.leads_df.columns if self.identify_column_type(col) == 'Update']
            if update_columns:
                print(f"Merging update columns: {update_columns}")
                
                def merge_updates(row):
                    updates = []
                    for col in update_columns:
                        value = row[col]
                        if pd.notna(value) and str(value).strip() != '':
                            updates.append(f"{col}: {str(value).strip()}")
                    return ' | '.join(updates) if updates else ''
                
                self.leads_df['Updates'] = self.leads_df.apply(merge_updates, axis=1)
                
                # Remove individual update columns
                self.leads_df = self.leads_df.drop(columns=update_columns)
            else:
                self.leads_df['Updates'] = ''
            
            # STEP 6: Reorder columns to have standard ones first
            standard_order = ['Name', 'Phone', 'Email', 'City', 'Updates']
            other_columns = [col for col in self.leads_df.columns if col not in standard_order]
            final_columns = standard_order + other_columns
            self.leads_df = self.leads_df[final_columns]
            
            # STEP 7: Remove exact duplicates (all columns must match exactly)
            initial_count = len(self.leads_df)
            self.leads_df = self.leads_df.drop_duplicates()
            duplicates_removed = initial_count - len(self.leads_df)
            print(f"Removed {duplicates_removed} exact duplicate leads records")
            
            # STEP 8: Fill NaN values with empty strings for cleaner display
            self.leads_df = self.leads_df.fillna('')
            
            self.processed_leads = self.leads_df.copy()
            
            return True, f"Processed {len(leads_files)} leads files, {len(self.processed_leads)} records, removed {duplicates_removed} duplicates"
            
        except Exception as e:
            return False, f"Error processing leads: {str(e)}"

    # ===== MAIN PROCESSING METHOD =====

    def process_all_files(self, folder_path):
        """Process both call logs and leads files from the same folder"""
        call_logs_success, call_logs_message = self.process_call_logs(folder_path)
        leads_success, leads_message = self.process_leads(folder_path)
        
        messages = []
        if call_logs_success:
            messages.append(f"✓ Call Logs: {call_logs_message}")
        else:
            messages.append(f"✗ Call Logs: {call_logs_message}")
            
        if leads_success:
            messages.append(f"✓ Leads: {leads_message}")
        else:
            messages.append(f"✗ Leads: {leads_message}")
        
        overall_success = call_logs_success or leads_success
        return overall_success, "\n".join(messages)

    # ===== AUTO-SAVE METHODS =====

    def auto_save_results(self, base_folder_path):
        """Automatically save both files to timestamped folder"""
        try:
            # Create timestamp for folder name
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_folder_name = f"Analysis_{timestamp}"
            output_folder_path = os.path.join(base_folder_path, output_folder_name)
            
            # Create the output folder
            os.makedirs(output_folder_path, exist_ok=True)
            
            saved_files = []
            
            # Save call logs if available
            if self.processed_call_logs is not None:
                call_logs_path = os.path.join(output_folder_path, "Processed_Call_Logs.xlsx")
                success, message = self.save_call_logs(call_logs_path)
                if success:
                    saved_files.append(f"✓ Call Logs: {os.path.basename(call_logs_path)}")
                else:
                    saved_files.append(f"✗ Call Logs: {message}")
            
            # Save leads if available
            if self.processed_leads is not None:
                leads_path = os.path.join(output_folder_path, "Processed_Leads.xlsx")
                success, message = self.save_leads(leads_path)
                if success:
                    saved_files.append(f"✓ Leads: {os.path.basename(leads_path)}")
                else:
                    saved_files.append(f"✗ Leads: {message}")
            
            return True, output_folder_path, saved_files
            
        except Exception as e:
            return False, "", [f"Error creating output folder: {str(e)}"]

    def save_call_logs(self, file_path):
        """Save processed call logs to Excel file"""
        if self.processed_call_logs is None:
            return False, "No processed call logs data to save"
        
        try:
            # Create export version - KEEP the 'All Dates and Times' column
            export_df = self.processed_call_logs.copy()
            export_df = export_df.drop(['Call Dates', 'Call Times'], axis=1)
            
            # Format dates
            export_df['First Call Date'] = export_df['First Call Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            export_df['Last Call Date'] = export_df['Last Call Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            export_df.to_excel(file_path, index=False)
            return True, f"Call logs saved to {file_path}"
            
        except Exception as e:
            return False, f"Error saving call logs file: {str(e)}"

    def save_leads(self, file_path):
        """Save processed leads to Excel file"""
        if self.processed_leads is None:
            return False, "No processed leads data to save"
        
        try:
            self.processed_leads.to_excel(file_path, index=False)
            return True, f"Leads saved to {file_path}"
            
        except Exception as e:
            return False, f"Error saving leads file: {str(e)}"

class UnifiedApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Unified Data Processor - Call Logs & Leads")
        self.root.geometry("1200x800")
        
        self.processor = UnifiedProcessor()
        
        self.setup_ui()
    
    def setup_ui(self):
        """Setup the user interface"""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Title
        title_label = ttk.Label(main_frame, text="Unified Data Processor", font=("Arial", 16, "bold"))
        title_label.grid(row=0, column=0, columnspan=2, pady=(0, 10))
        
        subtitle_label = ttk.Label(main_frame, text="Processes both Call Logs and Leads files from the same folder", font=("Arial", 10))
        subtitle_label.grid(row=1, column=0, columnspan=2, pady=(0, 10))
        
        # File selection
        file_frame = ttk.LabelFrame(main_frame, text="Folder Selection", padding="10")
        file_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Button(file_frame, text="Select Folder with Data Files", 
                 command=self.select_folder).grid(row=0, column=0, padx=5)
        
        self.file_label = ttk.Label(file_frame, text="No folder selected")
        self.file_label.grid(row=0, column=1, padx=5)
        
        # Process button
        ttk.Button(main_frame, text="Process All Files & Auto-Save", 
                 command=self.process_files).grid(row=3, column=0, pady=10)
        
        # Results frame with notebook for tabs
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.grid(row=4, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        
        # Call Logs tab
        self.call_logs_frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(self.call_logs_frame, text="Call Logs Results")
        
        # Leads tab
        self.leads_frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(self.leads_frame, text="Leads Results")
        
        # Create treeviews for both tabs
        self.setup_call_logs_treeview()
        self.setup_leads_treeview()
        
        # Status label
        self.status_label = ttk.Label(main_frame, text="Ready to process files", foreground="blue")
        self.status_label.grid(row=5, column=0, columnspan=2, pady=5)
        
        # Output folder label
        self.output_label = ttk.Label(main_frame, text="", foreground="green", font=("Arial", 9))
        self.output_label.grid(row=6, column=0, columnspan=2, pady=2)
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(4, weight=1)
    
    def setup_call_logs_treeview(self):
        """Setup treeview for call logs results"""
        # Treeview for call logs
        self.call_logs_tree = ttk.Treeview(self.call_logs_frame, columns=('Phone', 'Name', 'Total Calls', 'Total Duration', 'First Call', 'Last Call', 'Avg Gap'), show='headings')
        
        # Define headings
        self.call_logs_tree.heading('Phone', text='Phone Number')
        self.call_logs_tree.heading('Name', text='Name')
        self.call_logs_tree.heading('Total Calls', text='Total Calls')
        self.call_logs_tree.heading('Total Duration', text='Total Duration')
        self.call_logs_tree.heading('First Call', text='First Call')
        self.call_logs_tree.heading('Last Call', text='Last Call')
        self.call_logs_tree.heading('Avg Gap', text='Avg Gap (hours)')
        
        # Set column widths
        self.call_logs_tree.column('Phone', width=120)
        self.call_logs_tree.column('Name', width=150)
        self.call_logs_tree.column('Total Calls', width=80)
        self.call_logs_tree.column('Total Duration', width=100)
        self.call_logs_tree.column('First Call', width=120)
        self.call_logs_tree.column('Last Call', width=120)
        self.call_logs_tree.column('Avg Gap', width=100)
        
        # Scrollbar for treeview
        scrollbar = ttk.Scrollbar(self.call_logs_frame, orient=tk.VERTICAL, command=self.call_logs_tree.yview)
        self.call_logs_tree.configure(yscrollcommand=scrollbar.set)
        
        self.call_logs_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        self.call_logs_frame.columnconfigure(0, weight=1)
        self.call_logs_frame.rowconfigure(0, weight=1)
    
    def setup_leads_treeview(self):
        """Setup treeview for leads results"""
        # Treeview for leads
        self.leads_tree = ttk.Treeview(self.leads_frame, show='headings')
        
        # Scrollbars
        v_scrollbar = ttk.Scrollbar(self.leads_frame, orient=tk.VERTICAL, command=self.leads_tree.yview)
        h_scrollbar = ttk.Scrollbar(self.leads_frame, orient=tk.HORIZONTAL, command=self.leads_tree.xview)
        self.leads_tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        # Grid layout
        self.leads_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        v_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        h_scrollbar.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        self.leads_frame.columnconfigure(0, weight=1)
        self.leads_frame.rowconfigure(0, weight=1)
    
    def select_folder(self):
        """Select folder containing data files"""
        folder_path = filedialog.askdirectory(
            title="Select Folder with Call Logs and Leads Files"
        )
        
        if folder_path:
            self.file_label.config(text=os.path.basename(folder_path))
            self.current_folder = folder_path
            self.status_label.config(text="Folder selected. Click 'Process All Files & Auto-Save' to continue.", foreground="blue")
            self.output_label.config(text="")
    
    def process_files(self):
        """Process all files in the selected folder and auto-save results"""
        if not hasattr(self, 'current_folder'):
            messagebox.showwarning("Warning", "Please select a folder first")
            return
        
        self.status_label.config(text="Processing files...", foreground="orange")
        self.output_label.config(text="")
        self.root.update()
        
        # Process both call logs and leads
        success, message = self.processor.process_all_files(self.current_folder)
        
        if success:
            self.status_label.config(text="Processing completed! Auto-saving files...", foreground="orange")
            self.root.update()
            
            # Auto-save results to timestamped folder
            auto_save_success, output_folder_path, saved_files = self.processor.auto_save_results(self.current_folder)
            
            if auto_save_success:
                self.status_label.config(text="Processing and Auto-Save completed successfully!", foreground="green")
                self.output_label.config(text=f"Files saved to: {output_folder_path}", foreground="green")
                
                # Display results in preview
                if self.processor.processed_call_logs is not None:
                    self.display_call_logs_results(self.processor.processed_call_logs)
                
                if self.processor.processed_leads is not None:
                    self.display_leads_results(self.processor.processed_leads)
                
                # Show success message with saved files
                saved_files_message = "\n".join(saved_files)
                messagebox.showinfo("Processing Complete", 
                                  f"{message}\n\n"
                                  f"Files automatically saved to:\n{output_folder_path}\n\n"
                                  f"Saved files:\n{saved_files_message}")
            else:
                self.status_label.config(text="Processing completed but auto-save failed", foreground="red")
                messagebox.showerror("Auto-Save Error", 
                                   f"{message}\n\n"
                                   f"But auto-save failed:\n{saved_files[0]}")
        else:
            self.status_label.config(text="Processing failed", foreground="red")
            messagebox.showerror("Processing Error", message)
    
    def display_call_logs_results(self, results):
        """Display call logs results in the treeview"""
        # Clear existing data
        for item in self.call_logs_tree.get_children():
            self.call_logs_tree.delete(item)
        
        # Add new data
        for _, row in results.iterrows():
            self.call_logs_tree.insert('', tk.END, values=(
                row['Phone Number'],
                row['Name'],
                row['Total Calls'],
                row['Total Duration (HH:MM:SS)'],
                row['First Call Date'].strftime('%Y-%m-%d %H:%M') if pd.notna(row['First Call Date']) else 'N/A',
                row['Last Call Date'].strftime('%Y-%m-%d %H:%M') if pd.notna(row['Last Call Date']) else 'N/A',
                row['Avg Gap (hours)']
            ))
    
    def display_leads_results(self, results):
        """Display leads results in the treeview"""
        # Clear existing data
        for item in self.leads_tree.get_children():
            self.leads_tree.delete(item)
        
        # Clear existing columns
        for col in self.leads_tree["columns"]:
            self.leads_tree.heading(col, text="")
        
        # Configure treeview columns based on data
        self.leads_tree["columns"] = list(results.columns)
        for col in results.columns:
            self.leads_tree.heading(col, text=col)
            self.leads_tree.column(col, width=150, minwidth=100)
        
        # Add data to treeview
        for _, row in results.iterrows():
            values = []
            for col in results.columns:
                value = row[col]
                if pd.isna(value) or value == '':
                    values.append('')
                else:
                    values.append(str(value))
            self.leads_tree.insert('', tk.END, values=values)

def main():
    root = tk.Tk()
    app = UnifiedApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()