import os
import subprocess
import datetime

def process_one(s3_folder):

    source_bucket ='xxxINPUTBUCKETxxx'
    dest_bucket = 'xxxOUTPUTBUCKETxxx'
    bagit_profile = 'ucscbagit-v0.3.json'

    source_uri = "s3://" + source_bucket + "/" + s3_folder + "/"
    dest_uri = "s3://" + dest_bucket

    now = datetime.datetime.now()
    print(f"{now}: Begin {s3_folder}")

    try:
        subprocess.run(["aws","s3","sync", source_uri, s3_folder, "--no-progress", "--quiet"])

    except Exception as e:
        print(f"An error occurred while syncing: {e}")

    else:
        try:
            subprocess.run(["python3", "./bagit_profile.py", "--file", bagit_profile, bagit_profile, s3_folder])

        except Exception as e:
            print(f"An error occurred while validating bagit profile: {e}")

        try:
            subprocess.run(["zip","-0r", s3_folder+".zip", s3_folder])

        except Exception as e:
            print(f"An error occurred while zipping: {e}")

        else:
            try:
                subprocess.run(["aws","s3","cp", s3_folder+".zip", dest_uri, "--no-progress"])

            except Exception as e:
                print(f"An error occurred while uploading: {e}")

            else:
                try:
                    subprocess.run(["rm", s3_folder+".zip"])
                    subprocess.run(["rm", "-r", s3_folder])

                except Exception as e:
                    print(f"An error occurred while cleaning up: {e}")

    now = datetime.datetime.now()
    print(f"{now}: End {s3_folder}")

def main(filename):
    try:
        with open(filename, 'r') as file:
            for line in file:
                if line.strip() != "":
                    # zip and transfer one object
                    process_one(line.strip())
    except FileNotFoundError:
        print(f"The file {filename} does not exist.")
    except IOError as e:
        print(f"An IOError occurred: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python3 batch-zip-copy.py <input filename>. Input filename should have objects with one on each line.")
    else:
        main(sys.argv[1])
