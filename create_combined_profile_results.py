import glob

directories = [
    {'read_files': glob.glob("profile_results/top100values/*.csv"),
     'summary_file': "profile_summary/top100values.csv"
     },
    {'read_files': glob.glob("profile_results/number_column_profiles/*.csv"),
     'summary_file': "profile_summary/number_column_profiles.csv"
     },
    {'read_files': glob.glob("profile_results/string_column_profiles/*.csv"),
     'summary_file': "profile_summary/string_column_profiles.csv"
     }
]


for directory in directories:

    keep_header_row = True  # only keep the first header row from the first file to avoid header duplication
    with open(directory['summary_file'], "w") as outfile:
        for f in directory['read_files']:
            with open(f, "r") as infile:
                if keep_header_row:
                    line = infile.readline()
                    print(line, file=outfile, end='')
                    keep_header_row = False
                else:
                    infile.readline()  # throw away the first header line for subsequent files
                for line in infile:
                    print(line, file=outfile, end='')
