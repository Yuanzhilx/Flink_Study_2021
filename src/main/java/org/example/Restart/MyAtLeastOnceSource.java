package org.example.Restart;

import org.apache.commons.io.Charsets;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;


/***********************************
 *@Desc TODO
 *@ClassName MyAtLeastOnceSource
 *@Author DLX
 *@Data 2021/5/18 13:09
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class MyAtLeastOnceSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {
    private Long offset = 0L;
    private String path;
    private boolean flag = true;
    private transient ListState<Long> listState;

    public MyAtLeastOnceSource(String path) {
        this.path = path;
    }

    //相当于open方法在初始化状态或回复状态时执行一次
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> listStateDescriptor = new ListStateDescriptor<>("offset-state", Long.class);
        listState = context.getOperatorStateStore().getListState(listStateDescriptor);
        //当前的状态是否已经恢复了
        if (context.isRestored()){
            //从listState中恢复便宜量
            Iterable<Long> iterable = listState.get();
            for (Long aLong : iterable) {
                offset = aLong;
            }
        }
    }
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        RandomAccessFile randomAccessFile = new RandomAccessFile(path + "/" + indexOfThisSubtask + ".txt","r");
        randomAccessFile.seek(offset);//从指定位置读取数据
        while (flag){
            String line = randomAccessFile.readLine();
            if (line != null){
                line = new String(line.getBytes(Charsets.ISO_8859_1),Charsets.UTF_8);
                //对offset加锁，防止更新偏移量的时候进行checkPoint
                synchronized (ctx.getCheckpointLock()){
                    offset = randomAccessFile.getFilePointer();
                    ctx.collect(indexOfThisSubtask+".txt:"+line);
                }
            }else {
                Thread.sleep(1000);
            }
        }
    }
    //在做CheckPoint时周期性执行
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //定期更新OperatorState
        listState.clear();
        listState.add(offset);
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
