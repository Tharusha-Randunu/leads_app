import pandas as pd
import numpy as np
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os
import glob
import re

class LeadsProcessor:
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
    
    def identify_update_columns(self, df_columns):
        """Identify update/follow-up columns based on common naming patterns"""
        update_patterns = [
            # Numbered calls
            r'.*call.*1.*', r'.*1.*call.*', r'first.*call', r'call.*one',
            r'.*call.*2.*', r'.*2.*call.*', r'second.*call', r'call.*two',
            r'.*call.*3.*', r'.*3.*call.*', r'third.*call', r'call.*three',
            r'.*call.*4.*', r'.*4.*call.*', r'fourth.*call',
            r'.*call.*5.*', r'.*5.*call.*', r'fifth.*call',
            r'.*call.*6.*', r'.*6.*call.*', r'sixth.*call',
            r'.*call.*7.*', r'.*7.*call.*', r'seventh.*call',
            
            # Follow-ups
            r'.*follow.*up.*1.*', r'.*1.*follow.*up.*', r'first.*follow.*up',
            r'.*follow.*up.*2.*', r'.*2.*follow.*up.*', r'second.*follow.*up',
            r'.*follow.*up.*3.*', r'.*3.*follow.*up.*', r'third.*follow.*up',
            r'.*follow.*up.*4.*', r'.*4.*follow.*up.*', r'fourth.*follow.*up',
            r'.*follow.*up.*5.*', r'.*5.*follow.*up.*', r'fifth.*follow.*up',
            r'.*follow.*up.*6.*', r'.*6.*follow.*up.*', r'sixth.*follow.*up',
            r'.*follow.*up.*7.*', r'.*7.*follow.*up.*', r'seventh.*follow.*up',
            
            # Weekend calls
            r'weekend.*call', r'weekend.*follow.*up',
            
            # General update patterns
            r'update', r'followup', r'follow.*up', r'status'
        ]
        
        update_columns = []
        for col in df_columns:
            col_lower = str(col).lower()
            for pattern in update_patterns:
                if re.search(pattern, col_lower):
                    update_columns.append(col)
                    break
        
        return update_columns
    
    def merge_update_columns(self, row, update_columns):
        """Merge multiple update columns into one"""
        updates = []
        for col in update_columns:
            if pd.notna(row[col]) and str(row[col]).strip() != '':
                updates.append(f"{col}: {str(row[col]).strip()}")
        
        return ' | '.join(updates) if updates else ''
    
    def load_files_from_folder(self, folder_path):
        """Load and merge all leads files from a folder"""
        try:
            # Find all CSV and Excel files in the folder
            csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
            excel_files = glob.glob(os.path.join(folder_path, "*.xlsx"))
            excel_files.extend(glob.glob(os.path.join(folder_path, "*.xls")))
            
            all_files = csv_files + excel_files
            
            if not all_files:
                messagebox.showerror("Error", "No CSV or Excel files found in the selected folder")
                return False
            
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
                messagebox.showerror("Error", 
                    f"No files with 'leads' in filename found.\n"
                    f"Found {len(all_files)} files but none contain 'leads' in their names.")
                return False
            
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
                messagebox.showerror("Error", "No valid leads files could be loaded")
                return False
            
            # Merge all dataframes
            self.df = pd.concat(all_dfs, ignore_index=True)
            print(f"Total records after merging: {len(self.df)}")
            print(f"Original columns: {list(self.df.columns)}")
            
            # Identify required columns and update columns
            required_columns = []
            update_columns = self.identify_update_columns(self.df.columns)
            
            # Map common column names to standard names
            column_mapping = {}
            for col in self.df.columns:
                col_lower = str(col).lower()
                
                # Identify name columns
                if any(pattern in col_lower for pattern in ['name', 'fullname', 'full name', 'contact name']):
                    column_mapping[col] = 'Name'
                    required_columns.append('Name')
                
                # Identify email columns
                elif any(pattern in col_lower for pattern in ['email', 'e-mail', 'mail']):
                    column_mapping[col] = 'Email'
                    required_columns.append('Email')
                
                # Identify phone columns
                elif any(pattern in col_lower for pattern in ['phone', 'mobile', 'contact', 'number', 'telephone']):
                    column_mapping[col] = 'Phone'
                    required_columns.append('Phone')
                
                # Identify city columns
                elif any(pattern in col_lower for pattern in ['city', 'location', 'area']):
                    column_mapping[col] = 'City'
                    required_columns.append('City')
            
            # Rename columns
            self.df = self.df.rename(columns=column_mapping)
            
            # Keep only required columns and update columns
            columns_to_keep = list(set(required_columns)) + update_columns
            self.df = self.df[columns_to_keep]
            
            print(f"Columns after cleaning: {list(self.df.columns)}")
            print(f"Update columns found: {update_columns}")
            
            # Clean and standardize phone numbers
            if 'Phone' in self.df.columns:
                self.df['Phone'] = self.df['Phone'].apply(self.standardize_phone_number)
            
            # Merge update columns into one
            if update_columns:
                self.df['Updates'] = self.df.apply(
                    lambda row: self.merge_update_columns(row, update_columns), 
                    axis=1
                )
                # Remove individual update columns
                self.df = self.df.drop(columns=update_columns)
            
            # Remove exact duplicates after all processing
            initial_count = len(self.df)
            self.df = self.df.drop_duplicates()
            duplicates_removed = initial_count - len(self.df)
            print(f"Removed {duplicates_removed} exact duplicate records")
            
            # Remove rows where all required columns are empty
            required_cols = ['Name', 'Email', 'Phone', 'City']
            available_cols = [col for col in required_cols if col in self.df.columns]
            if available_cols:
                self.df = self.df.dropna(subset=available_cols, how='all')
            
            messagebox.showinfo("Success", 
                f"Successfully processed {len(leads_files)} leads files\n"
                f"Total records: {len(self.df)}\n"
                f"Exact duplicates removed: {duplicates_removed}\n"
                f"Final columns: {list(self.df.columns)}")
            return True
            
        except Exception as e:
            messagebox.showerror("Error", f"Error loading files from folder: {str(e)}")
            return False
    
    def process_data(self):
        """Return the processed data"""
        return self.df
    
    def save_results(self, file_path):
        """Save processed results to Excel file"""
        if self.df is None:
            messagebox.showerror("Error", "No processed data to save")
            return False
        
        try:
            self.df.to_excel(file_path, index=False)
            return True
            
        except Exception as e:
            messagebox.showerror("Error", f"Error saving file: {str(e)}")
            return False

class LeadsApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Leads Processor")
        self.root.geometry("1000x600")
        
        self.processor = LeadsProcessor()
        
        self.setup_ui()
    
    def setup_ui(self):
        """Setup the user interface"""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # File selection
        file_frame = ttk.LabelFrame(main_frame, text="Folder Selection", padding="10")
        file_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Button(file_frame, text="Select Folder with Leads Files", 
                 command=self.select_folder).grid(row=0, column=0, padx=5)
        
        self.file_label = ttk.Label(file_frame, text="No folder selected")
        self.file_label.grid(row=0, column=1, padx=5)
        
        # Process button
        ttk.Button(main_frame, text="Process Files", 
                 command=self.process_files).grid(row=1, column=0, pady=10)
        
        # Results frame
        results_frame = ttk.LabelFrame(main_frame, text="Processed Leads", padding="10")
        results_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        
        # Treeview for results
        self.tree = ttk.Treeview(results_frame, show='headings')
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Scrollbar for treeview
        scrollbar = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
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
        """Select folder containing leads files"""
        folder_path = filedialog.askdirectory(
            title="Select Folder with Leads Files"
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
        
        # Get processed data
        results = self.processor.process_data()
        
        if results is not None:
            self.display_results(results)
            messagebox.showinfo("Success", f"Processed {len(results)} leads records")
    
    def display_results(self, results):
        """Display results in the treeview"""
        # Clear existing data
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Configure treeview columns based on data
        self.tree["columns"] = list(results.columns)
        for col in results.columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=150)
        
        # Add data to treeview
        for _, row in results.iterrows():
            values = []
            for col in results.columns:
                value = row[col]
                if pd.isna(value):
                    values.append('')
                else:
                    values.append(str(value))
            self.tree.insert('', tk.END, values=values)
    
    def save_results(self):
        """Save processed results to file"""
        if self.processor.df is None:
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
    app = LeadsApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()