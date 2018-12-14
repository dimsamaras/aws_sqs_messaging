import subprocess

# subprocess.Popen('ls -la', shell=True)  
p1 = subprocess.Popen('dir', shell=True, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
p2 = subprocess.Popen('sort', shell=True, stdin=p1.stdout)
# out, err = p1.communicate()
p1.stdout.close()  
out, err = p2.communicate()  