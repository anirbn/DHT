package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;


import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;

import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


import static android.content.ContentValues.TAG;
import static edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider.KEY;
import static edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider.VALUE;

/**
 * References:
 * https://developer.android.com/reference/java/io/File.html
 * https://developer.android.com/guide/topics/data/data-storage.html#filesInternal
 * https://developer.android.com/reference/android/database/MatrixCursor.html
 * https://developer.android.com/reference/android/os/AsyncTask.html
 */

public class SimpleDhtProvider extends ContentProvider {

    //Anirban:start
    SQLiteDatabase database;
    DBHelper helper;
    private static final String TABLE_NAME = "messages";

    static final String basePort = "11108";
    static final int SERVER_PORT = 10000;
    public static final String KEY = "key";
    public static final String VALUE = "value";
    public Set<String> nodeKey = new TreeSet<String>();
    public TreeMap<String, String> nodeList = new TreeMap<String, String>();
    public HashMap<String, String> pair = new HashMap<String, String>();
    public String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};

    //Re-using from PA2A
    private final Uri CONTENT_URI = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    private ContentResolver contentResolver;
    ContentValues cv = new ContentValues(2);

    //Telephony Hack
    TelephonyManager tel;
    String portStr;
    String myPort;

    //Creating a node object
    class Node{
        private String port;
        Node prev;
        Node next;

        Node(String port) {
            this.port = port;
            this.prev = null;
            this.next = null;

        }

        public String getPort() {
            return port;
        }

        public Node getPrev() {
            return prev;
        }

        public void setPrev(Node prev) {
            this.prev = prev;
        }

        public Node getNext() {
            return next;
        }

        public void setNext(Node next) {
            this.next = next;
        }
    }

    Node node;

    //Anirban:end

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        try{
            boolean flag;
            if (!(selection.equals("*") || selection.equals("@"))){
                flag = getContext().deleteFile(selection);
            }
            else {
                File file  = getContext().getFilesDir();
                File f[] = file.listFiles();
                while (f.length!=0){
                    for (int i=0; i<f.length; i++){
                        flag = getContext().deleteFile(f[i].getName());
                    }
                }
                if (selection.equals("*")){
                    for (int k=0; k< REMOTE_PORTS.length; k++){
                        if (!(pair.get(REMOTE_PORTS[k]).equals(node.getPort()))){
                            if (nodeList.containsKey(genHash(pair.get(REMOTE_PORTS[k])))){
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(REMOTE_PORTS[k]));
                                PrintWriter client = new PrintWriter(socket.getOutputStream(), true);
                                client.println("DR:@");
                            }
                        }
                    }

                }
            }

        } catch (Exception e){
            Log.e(TAG, "delete");
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        //Anirban:start
        try{
            String keyToInsert = values.get(KEY).toString();
            String valueToInsert = values.get(VALUE).toString();
            String keyHash = genHash(keyToInsert);
            String myHash = genHash(node.getPort());
            String prevHash = genHash(node.getPrev().getPort());

            if (nodeKey.size() == 1 || ((keyHash.compareTo(myHash) < 0) && keyHash.compareTo(prevHash) > 0)){
                String string = valueToInsert + "\n";
                FileOutputStream outputStream;
                outputStream = getContext().openFileOutput(keyToInsert, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            }
            else {
                if ((myHash.compareTo(prevHash) < 0) && ((keyHash.compareTo(myHash) < 0) || (keyHash.compareTo(prevHash) > 0))){
                    String string = valueToInsert + "\n";
                    FileOutputStream outputStream;
                    outputStream = getContext().openFileOutput(keyToInsert, Context.MODE_PRIVATE);
                    outputStream.write(string.getBytes());
                    outputStream.close();
                }
                else {
                    String[] message = {keyToInsert, valueToInsert};
                    new ClientTaskInsert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                }
            }
        } catch (NoSuchAlgorithmException nsa) {
            Log.e(TAG, "No such algorithm");
        } catch (FileNotFoundException fnf){
            Log.e(TAG, "File not found");
        } catch (IOException ioe){
            Log.e(TAG, "IOException");
        }
        return null;
        //Anirban:end
    }

    @Override
    public boolean onCreate() {
        //Anirban:start
        /*helper = new DBHelper(this.getContext());*/
        contentResolver = getContext().getContentResolver();

        pair.put("11108", "5554");
        pair.put("11112", "5556");
        pair.put("11116", "5558");
        pair.put("11120", "5560");
        pair.put("11124", "5562");
        node = new Node(pair.get(getPort()));
        node.setNext(node);
        node.setPrev(node);
        if (!(getPort().equals(basePort))){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, basePort);
        }
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        return true;
        //Anirban:end

    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        String[] matrixColumns = {KEY, VALUE};
        MatrixCursor mc = new MatrixCursor(matrixColumns);
        //Anirban:start
        try{
            String keyToQuery = null;
            String originNode = node.getPort();

            if (selection.contains(":")){
                String ar[] = selection.split(":");
                keyToQuery = ar[1];
                originNode = ar[2];
            }
            else {
                keyToQuery = selection;
            }

            StringBuilder sb = new StringBuilder();
            String[] matrixColumns1 = {KEY, VALUE};
            MatrixCursor mco = new MatrixCursor(matrixColumns1);

            if (keyToQuery.equals("@") || (nodeKey.size()==1 && keyToQuery.equals("*"))){
                mco = new MatrixCursor(matrixColumns);
                File file = getContext().getFilesDir();
                File f[] = file.listFiles();
                for (int i=0; i< f.length; i++){
                    FileInputStream inputStream = getContext().openFileInput(f[i].getName());
                    InputStreamReader isr = new InputStreamReader(inputStream);
                    BufferedReader br = new BufferedReader(isr);
                    String line;

                    while ((line = br.readLine()) !=null){
                        sb.append(line);
                    }
                    mco.addRow(new Object[]{f[i].getName(), sb.toString()});
                    sb.delete(0,sb.toString().length());
                }
                return mco;
            }
            else if (keyToQuery.equals("*")) {
                mco = new MatrixCursor(matrixColumns);
                File file = getContext().getFilesDir();
                File f[] = file.listFiles();
                if (f.length!=0){
                    for (int i=0; i< f.length; i++){
                        FileInputStream inputStream = getContext().openFileInput(f[i].getName());
                        InputStreamReader isr = new InputStreamReader(inputStream);
                        BufferedReader br = new BufferedReader(isr);
                        String line;

                        while ((line = br.readLine()) !=null){
                            sb.append(line);
                        }

                        mco.addRow(new Object[]{f[i].getName(), sb.toString()});
                        sb.delete(0,sb.toString().length());
                    }

                }
                for (int k=0; k< REMOTE_PORTS.length; k++){
                    if (!(pair.get(REMOTE_PORTS[k]).equals(node.getPort()))){
                        if (nodeList.containsKey(genHash(pair.get(REMOTE_PORTS[k])))){
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(REMOTE_PORTS[k]));
                            PrintWriter client = new PrintWriter(socket.getOutputStream(), true);
                            client.println("QO:@:"+ originNode);
                            BufferedReader serverReceived = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String received = serverReceived.readLine();
                            if ((received != null) && !(received.equals("No Data"))) {
                                String ar[] = received.split("%");
                                for (int p=0; p<ar.length; p++){
                                    String temp[] = ar[p].split(":");
                                    mco.addRow(new Object[]{temp[0], temp[1]});
                                }
                            }
                            else {
                                continue;
                            }
                        }
                    }
                }
                return mco;
            }
            else {
                try {
                    FileInputStream inputStream = getContext().openFileInput(keyToQuery);
                    InputStreamReader isr = new InputStreamReader(inputStream);
                    BufferedReader br = new BufferedReader(isr);
                    String line;
                    while ((line = br.readLine()) !=null){
                        sb.append(line);
                    }

                    mco.addRow(new Object[]{keyToQuery, sb.toString()});
                    mco.close();
                    return mco;
                } catch (FileNotFoundException fnf) {
                        try {
                            String keyHash = genHash(keyToQuery);
                            boolean flag = false;
                            Iterator it = nodeKey.iterator();
                            while (it.hasNext()){
                                String temp = it.next().toString();
                                if (keyHash.compareTo(temp) < 0){
                                    String remotePort = String.valueOf(Integer.parseInt(nodeList.get(temp))*2);
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(remotePort));
                                    PrintWriter client = new PrintWriter(socket.getOutputStream(), true);
                                    client.println("QO:" + keyToQuery + ":" + originNode);
                                    BufferedReader serverReceived = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                    String received = serverReceived.readLine();
                                    if (received != null) {
                                        String ar[] = received.split(":");
                                        mc.addRow(new String[]{ar[0], ar[1]});
                                        flag = true;
                                        mc.close();
                                        return mc;
                                    }
                                }
                            }
                            if (!flag){
                                String remotePort = String.valueOf(Integer.parseInt(nodeList.get(nodeKey.toArray()[0].toString()))*2);
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(remotePort));
                                PrintWriter client = new PrintWriter(socket.getOutputStream(), true);
                                client.println("QO:" + keyToQuery + ":" + originNode);
                                BufferedReader serverReceived = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                String received = serverReceived.readLine();
                                if (received != null) {
                                    String ar[] = received.split(":");
                                    mc.addRow(new String[]{ar[0], ar[1]});
                                    mc.close();
                                    return mc;
                                }
                            }

                        } catch(Exception e){
                            Log.i("Inner", e.getMessage());
                        }
                    }
                }
        } catch (Exception e){
            Log.e("Query", e.getMessage());
        }finally{
            mc.close();
        }
        return null;
        //Anirban:end
    }

    public String queryFromOther(String key){
        StringBuilder sb = new StringBuilder();
        try {
            if (key.equals("@")){
                File file = getContext().getFilesDir();
                File f[] = file.listFiles();
                if (f.length==0){
                    return "No Data";
                }

                for (int i=0; i< f.length; i++){
                    FileInputStream inputStream = getContext().openFileInput(f[i].getName());
                    InputStreamReader isr = new InputStreamReader(inputStream);
                    BufferedReader br = new BufferedReader(isr);
                    String line;
                    sb.append(f[i].getName());
                    sb.append(":");
                    while ((line = br.readLine()) !=null){
                        sb.append(line);
                    }
                    sb.append("%");
                }
                sb.deleteCharAt(sb.lastIndexOf("$"));
            }
            else {
                FileInputStream inputStream = getContext().openFileInput(key);
                InputStreamReader isr = new InputStreamReader(inputStream);
                BufferedReader br = new BufferedReader(isr);
                String line;
                sb.append(key);
                sb.append(":");
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
            }
        } catch (FileNotFoundException fnf){
            Log.i("queryFromOther", "FileNotFoundException");
        } catch (IOException ioe){
            Log.i("queryFromOther", "IOException");
        } catch (Exception e){
            Log.i("queryFromOther", "Exception");
        }
        return sb.toString();
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    //Anirban:start
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public String getPort() {
        tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        return myPort;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            String input;
            ServerSocket serverSocket = sockets[0];

            try {
                //Adding self to the list
                String temp = pair.get(getPort());
                nodeKey.add(genHash(temp));
                nodeList.put(genHash(temp), temp);

                while (true) {
                    Socket server = serverSocket.accept();
                    BufferedReader serverInput = new BufferedReader
                            (new InputStreamReader(server.getInputStream()));
                    input = serverInput.readLine();
                    if (input != null){
                        String values[] = input.split(":");
                        String hash = genHash(values[1]);
                        if (values[0].equals("AN")){
                            if (!(nodeKey.contains(hash))){
                                nodeKey.add(hash);
                                nodeList.put(hash, values[1]);
                            }

                            Iterator i = nodeKey.iterator();
                            StringBuilder sb = new StringBuilder();
                            while(i.hasNext()){
                                String t = i.next().toString();
                                sb.append(nodeList.get(t));
                                sb.append(":");
                            }
                            sb.deleteCharAt(sb.lastIndexOf(":"));
                            PrintWriter client = new PrintWriter(server.getOutputStream(), true);
                            client.println(sb.toString());

                            //Setting previous and next
                            Object val[] = nodeKey.toArray();
                            if (val.length == 1 || val.length == 0){
                                node.setPrev(node);
                                node.setNext(node);
                            }
                            else {
                                for (int k=0; k<val.length; k++){
                                    if (node.getPort().equals(nodeList.get(val[k]))){
                                        if (k==0){
                                            node.setNext(new Node(nodeList.get(val[k+1])));
                                            node.setPrev(new Node(nodeList.get(val[val.length-1])));
                                        }
                                        else if (k==(val.length-1)){
                                            node.setNext(new Node(nodeList.get(val[0])));
                                            node.setPrev(new Node(nodeList.get(val[k-1])));
                                        }
                                        else {
                                            node.setNext(new Node(nodeList.get(val[k+1])));
                                            node.setPrev(new Node(nodeList.get(val[k-1])));
                                        }
                                    }
                                }
                            }
                        }
                        else if (values[0].equals("IR")){
                            insertToDB(values);
                        }
                        else if (values[0].equals("QR") || values[0].equals("QF")){
                            queryFromDB(values[0]+":"+values[1]+":"+values[2]);
                        }
                        else if(values[0].equals("QO")){
                            String val = queryFromOther(values[1]);
                            PrintWriter client = new PrintWriter(server.getOutputStream(), true);
                            client.println(val);
                        }
                        else if (values[0].equals("DR")){
                            deleteFromDB(values[1]);
                        }
                    }
                }
            } catch (IOException io){
                Log.e(TAG, "IOException");
            } catch (NoSuchAlgorithmException nsa){
                Log.e(TAG, "No Such Algorithm");
            }
            return null;
        }
    }

    private void insertToDB(String...strings){

        String keyToInsert = strings[1].trim();
        String valueToInsert = strings[2].trim();

        cv.put(KEY, keyToInsert);
        cv.put(VALUE, valueToInsert);
        contentResolver.insert(CONTENT_URI, cv);

        return;

    }

    private void queryFromDB(String value) {

        contentResolver.query(CONTENT_URI, null, value, null, null);

    }

    private void deleteFromDB(String keyToDelete) {

        contentResolver.delete(CONTENT_URI, keyToDelete, null);
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {
            try{
                String connectTo = strings[0];
                //Initiating connection to the base node which we know
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(connectTo));
                PrintWriter client = new PrintWriter(socket.getOutputStream(), true);
                //Creating and sending a connection request
                String temp = "AN:"+node.getPort();  //Alive Notification
                client.println(temp);
                //Getting back the response
                BufferedReader serverReceived = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String received = serverReceived.readLine();
                if (received != null){
                    String val[] = received.split(":");
                    for (int i=0; i < val.length; i++){
                        String hash = genHash(val[i]);
                        if (!(nodeKey.contains(hash))){
                            nodeKey.add(hash);
                            nodeList.put(hash, val[i]);
                        }
                    }
                    socket.close();
                }

                Iterator i = nodeKey.iterator();
                while(i.hasNext()){
                    String tempHash = i.next().toString();
                    if (!(tempHash.equals(genHash(pair.get(getPort()))) || tempHash.equals(genHash(pair.get(basePort))))){
                        Socket sock = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(String.valueOf(Integer.parseInt(nodeList.get(tempHash))*2)));
                        PrintWriter cl = new PrintWriter(sock.getOutputStream(), true);
                        cl.println(temp);
                    }
                }

                //Setting previous and next
                Object val[] = nodeKey.toArray();
                if (val.length == 1 || val.length == 0){
                    node.setPrev(node);
                    node.setNext(node);
                }
                else {
                    for (int k=0; k<val.length; k++){
                        if (node.getPort().equals(nodeList.get(val[k]))){
                            if (k==0){
                                node.setNext(new Node(nodeList.get(val[k+1])));
                                node.setPrev(new Node(nodeList.get(val[val.length-1])));
                            }
                            else if (k==(val.length-1)){
                                node.setNext(new Node(nodeList.get(val[0])));
                                node.setPrev(new Node(nodeList.get(val[k-1])));
                            }
                            else {
                                node.setNext(new Node(nodeList.get(val[k+1])));
                                node.setPrev(new Node(nodeList.get(val[k-1])));
                            }
                        }
                    }
                }
            } catch (Exception e){
                Log.e("Exception",e.getMessage());
            } finally {
                return null;
            }
        }
    }

    private class ClientTaskInsert extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {
            try{

                String keyToInsert = strings[0];
                String valueToInsert = strings[1];
                String sendToNode = String.valueOf(Integer.parseInt(node.getNext().getPort()) * 2);

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(sendToNode));
                PrintWriter client = new PrintWriter(socket.getOutputStream(), true);
                //Creating and sending an insert request
                String message = "IR:"+keyToInsert+":"+valueToInsert;
                client.println(message);
                //socket.close();
            } catch (Exception e){
                Log.e("ClientTaskInsert",e.getMessage());
            } finally {
                return null;
            }
        }
    }

    private class ClientTaskQuery extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {
            try{
                String keyToQuery = strings[0].trim();
                String originNode = strings[1].trim();

                String sendToNode = String.valueOf(Integer.parseInt(node.getNext().getPort()) * 2);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(sendToNode));
                PrintWriter client = new PrintWriter(socket.getOutputStream(), true);
                //Creating and sending an insert request
                String message = "QR:"+keyToQuery+":"+originNode;
                client.println(message);

            } catch (Exception e){
                Log.e("ClientTaskQuery",e.getMessage());
            } finally {
                return null;
            }
        }
    }

    private class ClientTaskDelete extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {
            try{
                String keyToDelete = strings[0].trim();
                String keyHash = genHash(keyToDelete);

                String sendToNode = null;

                Iterator it = nodeKey.iterator();
                while (it.hasNext()){
                    String temp = it.next().toString();
                    if (keyHash.compareTo(temp) < 0){
                        sendToNode = String.valueOf(Integer.parseInt(nodeList.get(temp))* 2);
                    }
                }

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(sendToNode));
                PrintWriter client = new PrintWriter(socket.getOutputStream(), true);
                //Creating and sending an insert request
                String message = "DR:"+keyToDelete;
                client.println(message);
                socket.close();
            } catch (Exception e){
                Log.e("ClientTaskDelete",e.getMessage());
            } finally {
                return null;
            }
        }
    }
}
