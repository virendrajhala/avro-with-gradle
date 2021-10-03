package com.fretron.service


import com.fretron.model.User
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import java.io.File


class UserService {

    fun serializeUser(user:User){

        val datumWriter = SpecificDatumWriter<User>()
        val dataFileWriter = DataFileWriter<User>(datumWriter)

        dataFileWriter.create(user.schema, File("./src/main/java/com/fretron/serializedUserFiles/user.avro"))

        dataFileWriter.append(user)

        dataFileWriter.close()
    }

    fun deserializeUser() : MutableList<User>{

        val datumReader = SpecificDatumReader<User>()
        val datafileReader = DataFileReader<User>(File("./src/main/java/com/fretron/serializedUserFiles/user.avro"),datumReader)

        val userList = mutableListOf<User>()
        while(datafileReader.hasNext()){
            val user = datafileReader.next()
            userList.add(user)
        }
        return userList
    }
}

fun main(){

    val user1 = User()
    val user2 = User(1, "Mark", "gurgaon","mark@gmail.com", 24)
    val user3 = User(2, "Roy","delhi", "roy.jason@gmail.com", 29)

    user1.setId(100)
    user1.setName("Virendra")
    user1.setEmail("vpsj@gmail.com")
    user1.setAge(23)
    user1.setAddress("noida")


    val userService = UserService()

//    userService.serializeUser()
    val userList = userService.deserializeUser()
    println(userList)
}