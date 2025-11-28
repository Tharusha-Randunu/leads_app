# app.py
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
import os
import sys
from datetime import datetime

# Add helpers to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'helpers'))

from data_cleaning import DataCleaner

class LeadAnalysisApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Lead Data Cleaner")
        self.root.geometry("700x500")
        
        self.cleaner = DataCleaner()
        self.current_output_folder = None
        
        self.setup_ui()
    
    def setup_ui(self):
        # Main frame
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        title_label = ttk.Label(main_frame, text="📁 Lead Data Cleaner", 
                               font=("Arial", 16, "bold"))
        title_label.pack(pady=10)
        
        # Instructions
        instructions = ttk.Label(main_frame, 
                                text="Select your main folder containing employee subfolders with lead files, update files, and call logs.",
                                wraplength=600, justify=tk.CENTER)
        instructions.pack(pady=5)
        
        # Folder selection
        folder_frame = ttk.LabelFrame(main_frame, text="1. Select Data Folder", padding="15")
        folder_frame.pack(fill=tk.X, pady=10)
        
        self.folder_path = tk.StringVar()
        
        folder_selection_frame = ttk.Frame(folder_frame)
        folder_selection_frame.pack(fill=tk.X)
        
        ttk.Entry(folder_selection_frame, textvariable=self.folder_path, width=60).pack(side=tk.LEFT, padx=5)
        ttk.Button(folder_selection_frame, text="Browse Folder", 
                  command=self.select_folder).pack(side=tk.LEFT, padx=5)
        
        # Process button
        process_frame = ttk.LabelFrame(main_frame, text="2. Process Data", padding="15")
        process_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(process_frame, text="🚀 Start Data Cleaning", 
                  command=self.process_data).pack(pady=10)
        
        # Progress
        self.progress = ttk.Progressbar(main_frame, mode='indeterminate')
        self.progress.pack(fill=tk.X, pady=5)
        
        # Results
        self.results_frame = ttk.LabelFrame(main_frame, text="3. Processing Results", padding="15")
        self.results_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        self.results_text = tk.Text(self.results_frame, height=12, wrap=tk.WORD)
        scrollbar = ttk.Scrollbar(self.results_frame, command=self.results_text.yview)
        self.results_text.config(yscrollcommand=scrollbar.set)
        
        self.results_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Action buttons (initially hidden)
        self.action_frame = ttk.Frame(main_frame)
    
    def select_folder(self):
        folder_selected = filedialog.askdirectory(title="Select Main Folder with Employee Subfolders")
        if folder_selected:
            self.folder_path.set(folder_selected)
            self.log_message(f"📁 Selected folder: {folder_selected}")
            self.log_message("   This folder should contain subfolders for each employee")
            self.log_message("   Each employee folder should contain:")
            self.log_message("   - Lead files (CSV/Excel with name, email, phone)")
            self.log_message("   - Update files (CSV/Excel with updates)")
            self.log_message("   - Call log files (CSV/Excel with call records)")
            self.log_message("")
    
    def process_data(self):
        if not self.folder_path.get():
            messagebox.showerror("Error", "Please select a folder first!")
            return
        
        try:
            self.progress.start()
            self.log_message("🔄 Starting data processing...")
            self.log_message("=" * 50)
            self.root.update()
            
            # Process data
            leads_df, updates_df, call_logs_df = self.cleaner.process_all_data(self.folder_path.get())
            
            # Save cleaned data to timestamped folder
            base_output_folder = os.path.join(os.path.dirname(__file__), 'results')
            output_folder = self.cleaner.save_cleaned_data(base_output_folder, leads_df, updates_df, call_logs_df)
            
            self.progress.stop()
            self.show_results(leads_df, updates_df, call_logs_df, output_folder)
            
        except Exception as e:
            self.progress.stop()
            self.log_message(f"❌ Error: {str(e)}")
            messagebox.showerror("Processing Error", f"An error occurred: {str(e)}")
    
    def show_results(self, leads_df, updates_df, call_logs_df, output_folder):
        self.log_message("\n🎉 DATA CLEANING COMPLETE!")
        self.log_message("=" * 50)
        
        # Show basic stats
        self.log_message(f"📊 FILES PROCESSED:")
        self.log_message(f"   ✅ Leads: {len(leads_df)} records")
        self.log_message(f"   ✅ Updates: {len(updates_df)} records")
        self.log_message(f"   ✅ Call Logs: {len(call_logs_df)} records")
        
        # Show city data info
        if 'city' in leads_df.columns:
            city_leads = leads_df['city'].notna().sum()
            self.log_message(f"   📍 Leads with city data: {city_leads}")
        
        if 'city' in updates_df.columns:
            city_updates = updates_df['city'].notna().sum()
            self.log_message(f"   📍 Updates with city data: {city_updates}")
        
        # Show sample data
        if not leads_df.empty:
            self.log_message(f"\n📋 SAMPLE LEADS:")
            for i, (_, lead) in enumerate(leads_df.head(3).iterrows()):
                city_info = f" | {lead['city']}" if 'city' in lead and pd.notna(lead['city']) else ""
                self.log_message(f"   {i+1}. {lead['name']} | {lead['phone']} | {lead['email']}{city_info}")
        
        if not updates_df.empty:
            self.log_message(f"\n📋 SAMPLE UPDATES:")
            for i, (_, update) in enumerate(updates_df.head(3).iterrows()):
                city_info = f" | {update['city']}" if 'city' in update and pd.notna(update['city']) else ""
                self.log_message(f"   {i+1}. {update['name']}{city_info} | {update['update_text'][:30]}...")
        
        # Show the exact folder path
        folder_name = os.path.basename(output_folder)
        self.log_message(f"\n💾 CLEANED FILES SAVED TO:")
        self.log_message(f"   📁 {output_folder}")
        self.log_message(f"   📄 cleaned_leads.csv")
        self.log_message(f"   📄 cleaned_updates.csv")
        self.log_message(f"   📄 cleaned_call_logs.csv")
        self.log_message(f"   📄 overall_performance.csv")
        self.log_message(f"\n⏰ Timestamp: {folder_name}")
        
        self.log_message(f"\n✅ Data is now ready for analysis!")
        
        # Store the output folder for the open button
        self.current_output_folder = output_folder
        
        # Show action buttons
        self.show_action_buttons()
    
    def show_action_buttons(self):
        # Clear existing buttons
        for widget in self.action_frame.winfo_children():
            widget.destroy()
        
        # Add new buttons
        ttk.Button(self.action_frame, text="📁 Open Results Folder", 
                  command=self.open_results_folder).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(self.action_frame, text="🔄 Process Another Folder", 
                  command=self.reset_app).pack(side=tk.LEFT, padx=5)
        
        self.action_frame.pack(pady=10)
    
    def open_results_folder(self):
        """Open the specific timestamped results folder"""
        if hasattr(self, 'current_output_folder') and os.path.exists(self.current_output_folder):
            os.startfile(self.current_output_folder)
        else:
            # Fallback: open the main results folder
            results_path = os.path.join(os.path.dirname(__file__), 'results')
            if os.path.exists(results_path):
                os.startfile(results_path)
            else:
                messagebox.showerror("Error", "Results folder not found!")
    
    def reset_app(self):
        """Reset the app to process another folder"""
        self.folder_path.set("")
        self.results_text.delete(1.0, tk.END)
        for widget in self.action_frame.winfo_children():
            widget.destroy()
        self.action_frame.pack_forget()
        self.current_output_folder = None
        self.log_message("🔄 Ready to process another folder...")
        self.log_message("Please select a new folder and click 'Start Data Cleaning'")
    
    def log_message(self, message):
        self.results_text.insert(tk.END, message + "\n")
        self.results_text.see(tk.END)
        self.root.update()

if __name__ == "__main__":
    # Check if pandas is installed
    try:
        import pandas as pd
    except ImportError:
        print("❌ pandas is not installed. Please run: pip install pandas")
        input("Press Enter to exit...")
        sys.exit(1)
    
    root = tk.Tk()
    app = LeadAnalysisApp(root)
    root.mainloop()