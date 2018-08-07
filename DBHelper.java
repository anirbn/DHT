package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by anirban on 4/9/18.
 */

//References
//SQLite: https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase.html

public class DBHelper extends SQLiteOpenHelper{

    public static final String KEY = "key";
    public static final String VALUE = "value";

    //private SQLiteDatabase database;
    public static final String DATABASE_NAME = "messageDB";
    public static final String TABLE_NAME = "messages";
    public static final int DATABASE_VERSION = 1;
    public static final String CREATE_DB_TABLE = " CREATE TABLE " + TABLE_NAME + "("
            + KEY + " TEXT PRIMARY KEY NOT NULL, "
            + VALUE + " TEXT NOT NULL)";


    public DBHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db){
        db.execSQL(CREATE_DB_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db,  int oldVersion, int newVersion) {
        //db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
        //onCreate(db);
    }

}
