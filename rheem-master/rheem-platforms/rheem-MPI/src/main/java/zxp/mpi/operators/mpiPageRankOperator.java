package zxp.mpi.operators;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.operators.PageRankOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.giraph.Algorithm.PageRankParameters;
import org.qcri.rheem.java.channels.StreamChannel;
import zxp.mpi.execution.mpiExecutor;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import mpi.*;

public class mpiPageRankOperator extends PageRankOperator implements mpiExecutionOperator {

    private String path_out;


    public mpiPageRankOperator(Integer numIterations) {
        super(numIterations);
        setPathOut(null,null);
    }

    public mpiPageRankOperator(PageRankOperator that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>,
            Collection<ChannelInstance>> evaluate(
                    ChannelInstance[] inputs,
                    ChannelInstance[] outputs,
                    mpiExecutor mpiExecutor,
                    OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length = this.getNumInputs();
        assert outputs.length = this.getNumOutputs();
        final FileChannel.Instance inputChannelInstance = (FileChannel.Instance) inputs[0];
        final StreamChannel.Instance outputChannelInstance = (StreamChannel.Instance) outputs[0];
        try {
            return this.runMpi(inputChannelInstance,outputChannelInstance,operatorContext);
        } catch (IOException e) {
            throw new RheemException(String.format("Running %s failed",this),e);
        }
    }

    private Tuple<Collection<ExecutionLineageNode>,Collection<ChannelInstance>> rumMpi(
            FileChannel.Instance inputFileChannelInstance,
            StreamChannel.Instance outputChannelInstance,
            OptimizationContext.OperatorContext operatorContext) throws IOException {

            Configuration configuration = operatorContext.getOptimizationContext().getConfiguration();
            MPIPageRank mpiPageRank = new (inputFileChannelInstance.getSinglePath(), PageRankParameters.getParameter(PageRankParameters.PageRankEnum.ITERATION),
                this.getDampingFactor());
            mpiPageRank.loadInput();
            mpiPageRank.calculatePageRank();

            String temDirPath = this.getPathOut(configuration);
            final Collection<String> actualInputPath = FileSystems.findActualInputPaths(temDirPath);
            Stream<Tuple<Long,Float>> stream = this.createStream(actualInputPath);
            outputChannelInstance.accept(stream);
            final ExecutionLineageNode mainExecutionLineage = new ExecutionLineageNode(operatorContext);
            return mainExecutionLineage.collectAndMark();

    }



    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_TSV_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    public String setPathOut(String path, Configuration configuration) {
        if(path == null && configuration != null) {
            path = configuration.getStringProperty("myrheem.mpi");
        }
        return this.path_out = path;
    }

    private String getPathOut(Configuration configuration) {
        if(this.path_out == null) {
            setPathOut(null,configuration);
        }
        return this.path_out;
    }

    public class MPIPageRank {
        // adjacency matrix read from file
        private HashMap<Integer, ArrayList<Integer>> adjMatrix = new HashMap<Integer, ArrayList<Integer>>();

        private String inputFile = "";

        private String outputFile = "";

        private int iterations = 10;

        private float dampingFactor = 0.85;

        private int numberofProcess = 4;
        private int rank = 0;
        private int size = 1;
        private int numofLine = 0;

        private double[] localPageRankValues, globalPageRankValue, danglingPageRankValue;


        public MPIPageRank(String inputFile,Integer iterations,Float dampingFactor,) {
            this.inputFile = inputFile;
            this.outputFile = outputFile;
            this.iterations = iterations;
            this.dampingFactor = dampingFactor;
            String[] args ={numberofProcess,inputFile,outputFile,iterations,dampingFactor};
            MPI.Init(args);
            rank = MPI.COMM_WORLD.Rank();
            size = MPI.COMM_WORLD.Size();
        }

        public int getrank() {
            return rank;
        }

        public void setFileLength() throws IOException {
            try (Stream<String> lines = Files.lines(Paths.get(inputFile), Charset.defaultCharset())) {
                numofLine = (int) lines.count();
            }

        }

        public void initializePageRankVariables() {
            globalPageRankValue = new double[numofLine];
            danglingPageRankValue = new double[numofLine];
            localPageRankValues = new double[numofLine];
            for (int i = 0; i < numofLine; i++) {
                double thesize = numofLine;
                globalPageRankValue[i] = 1 / thesize;
                danglingPageRankValue[i] = 0;
                localPageRankValues[i] = 0;
            }
        }

        public int getChunkSize() {
            return numofLine % numberofProcess == 0 ? numofLine / numberofProcess : numofLine / numberofProcess + 1;
        }

        public void inititalizeAdjMatrix(ArrayList<String> receivedChunk) {
            for (int i2 = 0; i2 < receivedChunk.size(); i2++) {
                String line = receivedChunk.get(i2);
                String split_line[] = line.split(" ");
                if (split_line.length > 1) {
                    ArrayList<Integer> nodes = new ArrayList<>();
                    for (int i = 1; i < split_line.length; i++)
                        nodes.add(Integer.parseInt(split_line[i]));
                    adjMatrix.put(Integer.parseInt(split_line[0]), nodes);
                } else if (split_line.length == 1)
                    adjMatrix.put(Integer.parseInt(split_line[0]), null);
            }
        }


        public void loadInput() throws IOException {

            setFileLength();


            initializePageRankVariables();

            try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {

                int chunkSize = getChunkSize();
                int count = 0;
                int senderIndex = 1;
                boolean fulfilledRank0 = false;
                ArrayList<String> chunkstrrecv = new ArrayList<String>();
                Object[] chunkstrrecvobj = new Object[1];
                if (rank == 0) {
                    System.out.println("Chunk Size:" + chunkSize);
                    ArrayList<String> chunkstr = new ArrayList<String>();
                    for (int i = 0; i < numofLine; i++) {
                        String tempstr = br.readLine();
                        chunkstr.add(tempstr);
                        count++;
                        if (count == chunkSize || i == numofLine - 1) {
                            if (!fulfilledRank0) {
                                chunkstrrecv.addAll(chunkstr);
                                fulfilledRank0 = true;
                            } else {
                                Object[] chunkstrobj = new Object[1];
                                chunkstrobj[0] = (Object) chunkstr;
                                MPI.COMM_WORLD.Send(chunkstrobj, 0, 1, MPI.OBJECT, senderIndex, 1);
                                senderIndex++;
                            }
                            chunkstr.clear();
                            count = 0;
                        }
                    }
                } else
                    MPI.COMM_WORLD.Recv(chunkstrrecvobj, 0, 1, MPI.OBJECT, 0, 1);
                if (rank != 0)
                    chunkstrrecv = (ArrayList<String>) chunkstrrecvobj[0];
                inititalizeAdjMatrix(chunkstrrecv);
            }
        }

        public void calculatePageRank() {

            for (int i = 0; i < iterations; i++) {
                for (int j = 0; j < numofLine; j++)
                    localPageRankValues[j] = 0.0;
                double danglingContrib = 0.0;
                Iterator it = adjMatrix.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<Integer, List> pair = (Map.Entry) it.next();
                    if (pair.getValue() == null)
                        danglingContrib += globalPageRankValue[pair.getKey()] / numofLine;
                    else {
                        int current_size = pair.getValue().size();
                        Iterator iter = pair.getValue().iterator();
                        while (iter.hasNext()) {
                            int node = Integer.parseInt(iter.next().toString());
                            double temp = localPageRankValues[node];
                            temp += globalPageRankValue[pair.getKey()] / current_size;
                            localPageRankValues[node] = temp;
                        }
                    }
                }
                double tempSend[] = new double[1];
                double tempRecv[] = new double[1];
                tempSend[0] = danglingContrib;
                MPI.COMM_WORLD.Allreduce(tempSend, 0, tempRecv, 0, 1, MPI.DOUBLE, MPI.SUM);
                MPI.COMM_WORLD.Allreduce(localPageRankValues, 0, globalPageRankValue, 0, numofLine, MPI.DOUBLE, MPI.SUM);
                if (rank == 0) {
                    for (int k = 0; k < numofLine; k++) {
                        globalPageRankValue[k] += tempRecv[0];
                        globalPageRankValue[k] = df * globalPageRankValue[k] + (1 - df) * (1.0 / (double) numofLine);
                    }
                }
                MPI.COMM_WORLD.Bcast(globalPageRankValue, 0, numofLine, MPI.DOUBLE, 0);
            }
        }

        public void printValues() throws IOException {
            for (int k = 0; k < numofLine; k++)
                System.out.println(k + ":" + globalPageRankValue[k]);
            HashMap<Integer, Double> pageRank = new HashMap<Integer, Double>();
            for (int i = 0; i < globalPageRankValue.length; i++) {
                pageRank.put(i, globalPageRankValue[i]);
            }
            Set<Map.Entry<Integer, Double>> set = pageRank.entrySet();
            List<Map.Entry<Integer, Double>> list = new ArrayList<Map.Entry<Integer, Double>>(set);
            Collections.sort(list, new Comparator<Map.Entry<Integer, Double>>() {
                public int compare(Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) {
                    return (o2.getValue()).compareTo(o1.getValue());
                }
            });
            try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(outputFile), "utf-8"))) {
                int i = 1;
                Iterator it = list.iterator();
                while (it.hasNext()) {
                    Map.Entry<Integer, Double> pair = (Map.Entry) it.next();
                    System.out.println(pair.getKey() + " :-  " + pair.getValue());
                    writer.write(pair.getKey() + " :-  " + pair.getValue() + "\n");
                    i++;
                    if (i == 11)
                        break;
                }
            }
        }

        private void returnjMat() {
            System.out.println("For AdjMatrix..." + rank);
            Iterator it = adjMatrix.values().iterator();
            while (it.hasNext()) {
                ArrayList<Integer> arr = (ArrayList<Integer>) it.next();
                if (arr != null) {
                    for (int i = 0; i < arr.size(); i++)
                        System.out.print(arr.get(i) + " ");
                    System.out.println();
                } else System.out.println("null");
            }
            MPI.Finalize();
        }

    }
}
