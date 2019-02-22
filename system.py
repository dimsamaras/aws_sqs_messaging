import os
import psutil

tot_m, used_m, free_m = map(int, os.popen('free -t -m').readlines()[-1].split()[1:])
print tot_m - used_m
psutil.cpu_percent()   
psutil.virtual_memory()    


#https://www.programcreek.com/python/example/13028/os.getloadavg
def check(self):
        data = {}
        # 20160725 windows???load??
        if platform.system() != 'Windows':
            load = os.getloadavg()
            data.update({'load.1': load[0], 'load.5': load[1], 'load.15': load[2]})
        data.update({"cpu.used_total": int(psutil.cpu_percent())})
        # ????CPU????
        per_cpu = psutil.cpu_percent(percpu=True)
        # ????CPU0???
        data.update({'cpu.cpu{0}_used'.format(i): int(val) for i,val in enumerate(per_cpu)})

        # ??CPU???
        new_cpu_times = psutil.cpu_times()
        if self.last_cpu_times is not None:
            last_total_time = reduce(lambda s,x:s+x, self.last_cpu_times)
            now_total_time = reduce(lambda s,x:s+x, new_cpu_times)
            total_time = now_total_time - last_total_time
            data['cpu.used_sy'] = self._get_cpu_time('system', total_time, new_cpu_times)
            data['cpu.used_us'] = self._get_cpu_time('user', total_time, new_cpu_times)
            data['cpu.used_wa'] = self._get_cpu_time('iowait', total_time, new_cpu_times)
            # data['cpu.used_id'] = self._get_cpu_time('idle', total_time, new_cpu_times)
            # data['cpu.used_ni'] = self._get_cpu_time('nice', total_time, new_cpu_times)
            # data['cpu.used_hi'] = self._get_cpu_time('irq', total_time, new_cpu_times)
            # data['cpu.used_si'] = self._get_cpu_time('softirq', total_time, new_cpu_times)
            # data['cpu.used_st'] = self._get_cpu_time('steal', total_time, new_cpu_times)
        else:# ?????
            self.last_cpu_times = new_cpu_times
            gevent.sleep(0.1)
            new_cpu_times = psutil.cpu_times()
            last_total_time = reduce(lambda s,x:s+x, self.last_cpu_times)
            now_total_time = reduce(lambda s,x:s+x, new_cpu_times)
            total_time = now_total_time - last_total_time
            data['cpu.used_sy'] = self._get_cpu_time('system', total_time, new_cpu_times)
            data['cpu.used_us'] = self._get_cpu_time('user', total_time, new_cpu_times)
            data['cpu.used_wa'] = self._get_cpu_time('iowait', total_time, new_cpu_times)

        self.last_cpu_times = new_cpu_times
        return data 