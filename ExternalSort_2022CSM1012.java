import java.io.*;
import java.util.*;

class UtilityFunctionClass
{
    static void copyFromMainToSecondaryMemory (List<String> data, int blockSize, int pass, int run, int part, boolean hasMoreParts, 
    Map<Integer, List<AbstractMap.SimpleEntry<Integer, String>>> index) throws IOException
    {
        if (data.size() == 0)
            return;
        
        File temp = new File ("secondaryMemory/pass_" + pass + "_run_" + run + "_data_part_" + part + ".txt");
        temp.createNewFile();
        FileWriter fwrite = new FileWriter(temp);

        for (int i = 0; i < blockSize; i++)
        {
            fwrite.write(data.get(i) + "\n");
        }

        if (hasMoreParts)
        {
            fwrite.write("secondaryMemory/pass_" + pass + "_run_" + run + "_data_part_" + (part+1) + ".txt");        
        }
        else
        {
            fwrite.write("END_OF_RUN");
        }

        if (part == 0)
        {
            AbstractMap.SimpleEntry<Integer, String> indexEntry = new AbstractMap.SimpleEntry<>(run, 
            "secondaryMemory/pass_" + pass + "_run_" + run + "_data_part_" + part + ".txt");

            if (!index.keySet().contains(pass))
                index.put(pass, new ArrayList<AbstractMap.SimpleEntry<Integer, String>>());
            
            index.get(pass).add(indexEntry);
        }

        fwrite.close();
    }

    static void cleanDirectory (File dir)
    {
        for (File file : dir.listFiles())
        {
            if (!file.isDirectory())
            {
                file.delete();
            }
        }
    }

    static List<String> sortDiskBlock (List<String> data)
    {
        List<String> sortedData = new ArrayList<>();
        Map<Integer, List<String>> mappedData = new HashMap<>();
        List<Integer> transactionData = new ArrayList<>();

        for (String str : data)
        {
            String[] temp = str.split(" ");
            int tempInt = Integer.parseInt(temp[1]);
            transactionData.add(tempInt);
            if (mappedData.containsKey(tempInt))
            {
                mappedData.get(tempInt).add(str);
            }
            else
            {
                List<String> strArr = new ArrayList<>();
                strArr.add(str);
                mappedData.put(tempInt, strArr);
            }
        }

        Collections.sort(transactionData);

        for (int x : transactionData)
        {
            sortedData.add(mappedData.get(x).get(0));
            mappedData.get(x).remove(0);
        }

        return sortedData;
    }
}

class InitialFileCreation
{
    /*
     * generates 50,000 random records and stores them in a single text file
     */
    public static void generateInitialData () throws IOException
    {
        UtilityFunctionClass.cleanDirectory(new File ("secondaryMemory"));

        File original = new File("original.txt");
        original.createNewFile();
        if (original.exists())
        {
            Random random = new Random();
            FileWriter fwrite = new FileWriter(original);
            for (int index = 1; index <= 50000; index++)
            {
                int saleAmount = random.nextInt(60001);
                String customerName = generateRandomString();
                int category = random.nextInt(1501);

                fwrite.write(index + " " + saleAmount + " " + customerName + " " + category + "\n");
            }
            fwrite.close();
        }
    }

    private static String generateRandomString ()
    {
        StringBuilder buffer = new StringBuilder(3);
        int alphabetStart = 97;
        int alphabetEnd = 122;
        for (int i = 0; i < 3; i++)
        {
            int randomInt = (int) (Math.random() * (alphabetEnd - alphabetStart + 1)) + alphabetStart;
            buffer.append((char) randomInt);
        }

        return (buffer.toString());
    }

    /*
     * breaks down the single text file of records into multiple text files of user given size
     * and links them together using pointers to simulate the storage of large files in secondary      memory
     * 
     * @params { data } all the records from the single original text file
     * @params { blockSize } number of records that can be stored in one memory block
     * @params { pass }
     * 
     * @returns { String } path to the first disk block containing the initial unsorted data
     */
    static String simulateDiskBlocksWithInitialData (List<String> data, int blockSize, int pass) throws IOException
    {
        int curRun = 0;
        int totalRuns = (int) Math.ceil((double) data.size() / (double) blockSize);
        while (curRun < totalRuns)
        {
            File temp = new File ("secondaryMemory/initial_unsorted_data_pass_" + pass + "_run_" + curRun + ".txt");
            temp.createNewFile();
            FileWriter fwrite = new FileWriter(temp);

            if ((curRun+1)*blockSize < data.size())
            {
                for (int i = 0; i < blockSize; i++)
                {
                    fwrite.write(data.get(curRun*blockSize + i) + "\n");
                }
                fwrite.write("secondaryMemory/initial_unsorted_data_pass_" + pass + "_run_" + (curRun+1) + ".txt");
            }
            else
            {
                for (int i = curRun*blockSize; i < data.size(); i++)
                {
                    fwrite.write(data.get(i) + "\n");
                }
                fwrite.write("END_OF_BLOCKS");
            }
            curRun++;
            fwrite.close();
        }

        return ("secondaryMemory/initial_unsorted_data_pass_" + pass + "_run_" + 0 + ".txt");
    }
}

class MinHeapNode
{
    String record;
    int diskBlock;
    int nextRecordIndex;
    int transactionValue;

    MinHeapNode (String record, int diskBlock, int nextRecordIndex)
    {
        this.record = record;
        this.diskBlock = diskBlock;
        this.nextRecordIndex = nextRecordIndex;
        
        String[] temp = record.split(" ");
        this.transactionValue = Integer.parseInt(temp[1]);
    }
}

class MinHeap
{
    MinHeapNode[] heapArr;
    int heapSize;

    MinHeap (MinHeapNode[] arr, int size)
    {
        this.heapArr = arr;
        this.heapSize = size;

        int i = (heapSize-1) / 2;
        while(i >= 0)
        {
            minHeapify(i--);
        }
    }

    void minHeapify (int index)
    {
        int leftChildIndex = (2 * index) + 1;
        int rightChildIndex = (2 * index) + 2;
        int minimum = index;

        if (leftChildIndex < heapSize && heapArr[leftChildIndex].transactionValue < heapArr[index].transactionValue)
            minimum = leftChildIndex;

        if (rightChildIndex < heapSize && heapArr[rightChildIndex].transactionValue < heapArr[minimum].transactionValue)
            minimum = rightChildIndex;
        
        if (minimum != index)
        {
            swapNodes(index, minimum);
            minHeapify(minimum);
        }
    }

    MinHeapNode extractMin ()
    {
        return this.heapArr[0];
    }

    void insertAndHeapify (MinHeapNode newNode)
    {
        this.heapArr[0] = newNode;
        minHeapify(0);
    }

    void swapNodes (int i, int j)
    {
        MinHeapNode temp = heapArr[i];
        heapArr[i] = heapArr[j];
        heapArr[j] = temp;
    }
}

public class ExternalSort_2022CSM1012
{
    // outer list contains 'mainMemSize' inner lists - representing disk blocks
    // each inner list contains 'blockSize' records - representing records in a disk block
    static List<List<String>> simulatedMainMemory = new ArrayList<List<String>>();
    
    // index file containing the list of links to first files of a run in a pass
    static Map<Integer, List<AbstractMap.SimpleEntry<Integer, String>>> index = new HashMap<Integer, List<AbstractMap.SimpleEntry<Integer, String>>>();

    public static void main (String[] args) throws IOException
    {
        InitialFileCreation.generateInitialData();

        Scanner userInput = new Scanner(System.in);
        File file = new File ("original.txt");
        Scanner fRead = new Scanner (file);
        List<String> fReadData = new ArrayList<>();

        while (fRead.hasNextLine())
        {
            fReadData.add(fRead.nextLine());
        }

        System.out.println("Enter the number of records in a disk block (B)");
        int blockSize = userInput.nextInt();
        System.out.println("Enter the number of disk blocks in main memory (M)");
        int mainMemSize = userInput.nextInt();

        initialiseMainMemory(mainMemSize);

        String firstFileName = InitialFileCreation.simulateDiskBlocksWithInitialData(fReadData, blockSize, 0);

        userInput.close();
        fRead.close();

        externalSort(firstFileName, blockSize, mainMemSize);
    }

    static void externalSort (String firstFileName, int blockSize, int mainMemSize) throws FileNotFoundException, IOException
    {
        index.clear();
        index.put(0, new ArrayList<AbstractMap.SimpleEntry<Integer, String>>());

        applySortingToInitialRuns (firstFileName, blockSize, mainMemSize);
        int pass = 0;

        // case 2 : number of runs in the pass > number of main memory blocks - 1
        while (index.get(pass).size() > mainMemSize-1)
        {
            int nextPassRunCount = 0;

            for (int run = 0; run < index.get(pass).size(); )
            {
                List<String> nextFilePointers = new ArrayList<>();
                int i = mainMemSize-1;
                while ((i > 0) && (run < index.get(pass).size()))
                {
                    nextFilePointers.add(index.get(pass).get(run).getValue());
                    i--; run++;
                }

                kWayMerge(nextFilePointers, blockSize, mainMemSize, pass, nextPassRunCount++);
            }

            pass++;
        }
        
        // case 1 : number of runs in the pass <= number of main memory blocks - 1
        for (int run = 0; run < index.get(pass).size(); )
        {
            List<String> nextFilePointers = new ArrayList<>();
            int i = mainMemSize-1;
            while ((i > 0) && (run < index.get(pass).size()))
            {
                nextFilePointers.add(index.get(pass).get(run).getValue());
                i--; run++;
            }

            kWayMerge(nextFilePointers, blockSize, mainMemSize, pass, 0);
        }

        combineFinalOutput(index.get(pass+1).get(0).getValue(), blockSize);
    }

    static void kWayMerge(List<String> nextFilePointers, int blockSize, int mainMemSize, int pass, int run) throws FileNotFoundException, 
    IOException
    {
        clearMainMemory(-1);

        for (int i = 0; i < nextFilePointers.size(); i++)
        {
            nextFilePointers.set(i, copyToMainMemory(nextFilePointers.get(i), i));
        }

        int heapSize = Math.min(nextFilePointers.size(), mainMemSize-1);
        MinHeapNode[] heapArr = new MinHeapNode[heapSize];

        for (int i = 0; i < heapSize; i++)
        {
            MinHeapNode newNode = new MinHeapNode(simulatedMainMemory.get(i).get(0), i, 1);
            heapArr[i] = newNode;
        }

        MinHeap minHeap = new MinHeap(heapArr, heapSize);
        int part = 0;
        int outputBlock = mainMemSize-1;

        while (minHeap.heapSize > 0)
        {
            MinHeapNode minNode = minHeap.extractMin();
            MinHeapNode newNode;
            simulatedMainMemory.get(outputBlock).add(minNode.record);            
            
            int response = isMoreDataInBlock(minNode.diskBlock, minNode.nextRecordIndex, blockSize, nextFilePointers);
            if(response == 1)
            {
                // make new Node with the next record in the same block, replace root with it, and minheapify
                newNode = new MinHeapNode(simulatedMainMemory.get(minNode.diskBlock).get(minNode.nextRecordIndex), 
                minNode.diskBlock, minNode.nextRecordIndex+1); 
                minHeap.insertAndHeapify(newNode);
            }
            else if (response == -2)
            {
                minHeap.heapArr[0] = minHeap.heapArr[minHeap.heapSize-1];
                minHeap.minHeapify(0);
                minHeap.heapSize -= 1;
            }
            else
            {
                // make new node with 0th record of the same block, replace root with it, minheapify
                newNode = new MinHeapNode(simulatedMainMemory.get(minNode.diskBlock).get(0), minNode.diskBlock, 1);
                minHeap.insertAndHeapify(newNode);
            }

            part = checkOutputBlockFull(outputBlock, blockSize, pass, run, part, (minHeap.heapSize > 0));
        }

        System.out.println();
    }

    /*
     * finds whether the block in main memory has more records to fetch or not
     * 
     * @params { diskBlock } index of main memory block that needs to be checked
     * @params { nextRecordIndex } index of next record that needs to be added to minHeap
     * @params { blockSize } 
     * @params { nextFilePointers } list containing links to next disk block that should be fetched into main memory if all the records of
     * current disk block have been processed
     * 
     * @returns { integer } 1 : if there are more records to be fetched in current disk block
     * @returns { integer } -2 : if the current disk block is exhausted and there are no more disk blocks to be loaded into main memory from
     * that run
     * @returns { integer } 0 : if the current disk block is exhausted and the next disk block has been fetched in the main memory
     */
    static int isMoreDataInBlock (int diskBlock, int nextRecordIndex, int blockSize, List<String> nextFilePointers)
    throws FileNotFoundException
    {
        if (nextRecordIndex < simulatedMainMemory.get(diskBlock).size())
            return 1;
        
        if (nextFilePointers.get(diskBlock).equals("END_OF_RUN"))
            return -2;

        nextFilePointers.set(diskBlock, copyToMainMemory(nextFilePointers.get(diskBlock), diskBlock));
        return 0;
    }

    /*
     * checks whether the outputBlock in main memory is full
     * if it is full, then the content is copied from main memory to secondary memory and the disk block is emptied
     */
    static int checkOutputBlockFull (int outputBlock, int blockSize, int pass, int run, int part, boolean hasMoreParts)
    throws IOException
    {
        if (simulatedMainMemory.get(outputBlock).size() < blockSize)
            return part;
        
        UtilityFunctionClass.copyFromMainToSecondaryMemory(simulatedMainMemory.get(outputBlock), blockSize, pass+1, run, part, hasMoreParts, index);

        clearMainMemory(outputBlock);

        return part+1;
    }

    /*
     * copies 'mainMemSize' disk blocks from secondary memory to main memory, sorts each main memory block and writes the sorted
     * blocks back to secondary memory
     */
    static void applySortingToInitialRuns (String firstFileName, int blockSize, int mainMemSize) throws FileNotFoundException, IOException
    {
        String nextBlockPtr = firstFileName;
        int run = 0;
                
        while (!nextBlockPtr.equals("END_OF_BLOCKS"))
        {
            int tempCounter = 0;
            clearMainMemory(-1);

            while (tempCounter < mainMemSize && !nextBlockPtr.equals("END_OF_BLOCKS"))
            {
                File file = new File (nextBlockPtr);
                Scanner fRead = new Scanner (file);
                List<String> fReadData = new ArrayList<>();

                while (fRead.hasNextLine())
                {
                    fReadData.add(fRead.nextLine());
                }

                nextBlockPtr = fReadData.get(fReadData.size()-1);
                fReadData.remove(fReadData.size()-1);

                simulatedMainMemory.set(tempCounter++, fReadData);

                fRead.close();
            }

            for (int i = 0; i < simulatedMainMemory.size(); i++)
            {
                simulatedMainMemory.set(i, UtilityFunctionClass.sortDiskBlock(simulatedMainMemory.get(i)));
                UtilityFunctionClass.copyFromMainToSecondaryMemory(simulatedMainMemory.get(i), blockSize, 0, run++, 0, false, index);
            }
        }
    }

    /*
     * copies a disk block from secondary memory to main memory
     * 
     * @params { fileName } path to the disk block in secondary memory that needs to be copied
     * @params { mainMemBlock } index of the main memory block in which the data needs to be copied
     * 
     * @returns { String } returns the link to next disk block in secondary memory
     */
    static String copyToMainMemory (String fileName, int mainMemBlock) throws FileNotFoundException
    {
        File file = new File (fileName);
        Scanner fRead = new Scanner (file);
        List<String> fReadData = new ArrayList<>();

        while (fRead.hasNextLine())
        {
            fReadData.add(fRead.nextLine());
        }

        String temp = fReadData.get(fReadData.size()-1);
        fReadData.remove(fReadData.size()-1);

        simulatedMainMemory.set(mainMemBlock, fReadData);
        fRead.close();
        return temp;
    }

    /*
     * clear the data in main memory
     * 
     * @params { block } -1 : clears all the disk blocks in main memory
     * @params { block } integer >= 0 : only clears the main memory disk block which is at index 'block'
     */
    static void clearMainMemory (int block)
    {
        if (block == -1)
        {
            for (int i = 0; i < simulatedMainMemory.size(); i++)
                simulatedMainMemory.get(i).clear();
        }
        else
        {
            simulatedMainMemory.get(block).clear();
        }
    }

    static void initialiseMainMemory (int mainMemSize)
    {
        for (int i = 0; i < mainMemSize; i++)
        {
            List<String> temp = new ArrayList<>();
            simulatedMainMemory.add(temp);
        }
    }

    /*
     * combines the parts of final pass containing sorted records into a single text file
     */
    static void combineFinalOutput (String filePtr, int blockSize) throws IOException
    {
        File output = new File ("output.txt");
        output.createNewFile();
        if (output.exists())
        {
            FileWriter fwrite = new FileWriter(output);

            while (!filePtr.equals("END_OF_RUN"))
            {
                File file = new File (filePtr);
                Scanner fRead = new Scanner (file);

                for (int i = 0; i < blockSize; i++)
                {
                    fwrite.append(fRead.nextLine() + "\n");
                }

                filePtr = fRead.nextLine();

                fRead.close();
            }

            fwrite.close();
        }
    }
}