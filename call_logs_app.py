import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os
import glob
import re

class CallLogProcessor:
    def __init__(self):
        self.df = None
        self.processed_data = None
        
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
    
    def load_files_from_folder(self, folder_path):
        """Load and merge all call log files from a folder"""
        try:
            # Find all CSV files in the folder
            csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
            
            if not csv_files:
                messagebox.showerror("Error", "No CSV files found in the selected folder")
                return False
            
            # List to store all dataframes
            all_dfs = []
            
            for file_path in csv_files:
                try:
                    # Read each CSV file
                    df = pd.read_csv(file_path)
                    print(f"Loaded file: {os.path.basename(file_path)} with {len(df)} records")
                    all_dfs.append(df)
                except Exception as e:
                    print(f"Error loading file {os.path.basename(file_path)}: {str(e)}")
                    continue
            
            if not all_dfs:
                messagebox.showerror("Error", "No valid CSV files could be loaded")
                return False
            
            # Merge all dataframes
            self.df = pd.concat(all_dfs, ignore_index=True)
            print(f"Total records after merging: {len(self.df)}")
            
            # Basic cleaning
            self.df = self.df.dropna(subset=['To Number'])  # Remove rows with no To Number
            self.df = self.df[self.df['To Number'].notna()]
            
            # Convert Date Time column to datetime
            self.df['Date Time'] = pd.to_datetime(self.df['Date Time'], errors='coerce')
            
            # Clean and standardize phone numbers
            self.df['To Number'] = self.df['To Number'].apply(self.standardize_phone_number)
            
            # Remove scientific notation numbers and invalid phone numbers
            self.df = self.df[~self.df['To Number'].str.contains('E', na=False)]
            self.df = self.df[self.df['To Number'].str.len() >= 9]  # Minimum 9 digits (94 + 7)
            
            # Convert Duration to seconds
            self.df['Duration_Seconds'] = self.df['Duration'].apply(self.parse_duration)
            
            # REMOVE DUPLICATES AFTER ALL CLEANING AND MERGING - MOVED THIS STEP
            initial_count = len(self.df)
            self.df = self.df.drop_duplicates()
            duplicates_removed = initial_count - len(self.df)
            print(f"Removed {duplicates_removed} exact duplicate records")
            
            messagebox.showinfo("Success", f"Successfully merged {len(csv_files)} files\nTotal records: {len(self.df)}\nExact duplicates removed: {duplicates_removed}")
            return True
            
        except Exception as e:
            messagebox.showerror("Error", f"Error loading files from folder: {str(e)}")
            return False
    
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
    
    def process_data(self):
        """Process the data and create records for each phone number"""
        if self.df is None:
            return None
        
        records = []
        
        # Group by phone number
        for phone_number, group in self.df.groupby('To Number'):
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
        
        self.processed_data = pd.DataFrame(records)
        return self.processed_data
    
    def save_results(self, file_path):
        """Save processed results to Excel file"""
        if self.processed_data is None:
            messagebox.showerror("Error", "No processed data to save")
            return False
        
        try:
            # Create export version - KEEP the 'All Dates and Times' column
            export_df = self.processed_data.copy()
            export_df = export_df.drop(['Call Dates', 'Call Times'], axis=1)
            
            # Format dates
            export_df['First Call Date'] = export_df['First Call Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            export_df['Last Call Date'] = export_df['Last Call Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            export_df.to_excel(file_path, index=False)
            return True
            
        except Exception as e:
            messagebox.showerror("Error", f"Error saving file: {str(e)}")
            return False

class CallLogApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Call Log Processor")
        self.root.geometry("900x600")
        
        self.processor = CallLogProcessor()
        
        self.setup_ui()
    
    def setup_ui(self):
        """Setup the user interface"""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # File selection
        file_frame = ttk.LabelFrame(main_frame, text="Folder Selection", padding="10")
        file_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Button(file_frame, text="Select Folder with Call Logs", 
                 command=self.select_folder).grid(row=0, column=0, padx=5)
        
        self.file_label = ttk.Label(file_frame, text="No folder selected")
        self.file_label.grid(row=0, column=1, padx=5)
        
        # Process button
        ttk.Button(main_frame, text="Process Files", 
                 command=self.process_files).grid(row=1, column=0, pady=10)
        
        # Results frame
        results_frame = ttk.LabelFrame(main_frame, text="Results", padding="10")
        results_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        
        # Treeview for results
        self.tree = ttk.Treeview(results_frame, columns=('Phone', 'Name', 'Total Calls', 'Total Duration', 'First Call', 'Last Call', 'Avg Gap'), show='headings')
        
        # Define headings
        self.tree.heading('Phone', text='Phone Number')
        self.tree.heading('Name', text='Name')
        self.tree.heading('Total Calls', text='Total Calls')
        self.tree.heading('Total Duration', text='Total Duration')
        self.tree.heading('First Call', text='First Call')
        self.tree.heading('Last Call', text='Last Call')
        self.tree.heading('Avg Gap', text='Avg Gap (hours)')
        
        # Set column widths
        self.tree.column('Phone', width=120)
        self.tree.column('Name', width=150)
        self.tree.column('Total Calls', width=80)
        self.tree.column('Total Duration', width=100)
        self.tree.column('First Call', width=120)
        self.tree.column('Last Call', width=120)
        self.tree.column('Avg Gap', width=100)
        
        # Scrollbar for treeview
        scrollbar = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # Save button
        ttk.Button(main_frame, text="Save Results", 
                 command=self.save_results).grid(row=3, column=0, pady=10)
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(2, weight=1)
        results_frame.columnconfigure(0, weight=1)
        results_frame.rowconfigure(0, weight=1)
    
    def select_folder(self):
        """Select folder containing call log files"""
        folder_path = filedialog.askdirectory(
            title="Select Folder with Call Log Files"
        )
        
        if folder_path:
            self.file_label.config(text=os.path.basename(folder_path))
            self.current_folder = folder_path
    
    def process_files(self):
        """Process all files in the selected folder"""
        if not hasattr(self, 'current_folder'):
            messagebox.showwarning("Warning", "Please select a folder first")
            return
        
        # Load files from folder
        if not self.processor.load_files_from_folder(self.current_folder):
            return
        
        # Process data
        results = self.processor.process_data()
        
        if results is not None:
            self.display_results(results)
            messagebox.showinfo("Success", f"Processed {len(results)} unique phone numbers")
    
    def display_results(self, results):
        """Display results in the treeview"""
        # Clear existing data
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Add new data
        for _, row in results.iterrows():
            self.tree.insert('', tk.END, values=(
                row['Phone Number'],
                row['Name'],
                row['Total Calls'],
                row['Total Duration (HH:MM:SS)'],
                row['First Call Date'].strftime('%Y-%m-%d %H:%M') if pd.notna(row['First Call Date']) else 'N/A',
                row['Last Call Date'].strftime('%Y-%m-%d %H:%M') if pd.notna(row['Last Call Date']) else 'N/A',
                row['Avg Gap (hours)']
            ))
    
    def save_results(self):
        """Save processed results to file"""
        if self.processor.processed_data is None:
            messagebox.showwarning("Warning", "No data to save. Please process files first.")
            return
        
        file_path = filedialog.asksaveasfilename(
            title="Save Results As",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        
        if file_path:
            if self.processor.save_results(file_path):
                messagebox.showinfo("Success", f"Results saved to {file_path}")

def main():
    root = tk.Tk()
    app = CallLogApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()