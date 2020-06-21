package Util.utils

import java.util.ResourceBundle

object PropertiesUtil {

 val summer: ResourceBundle = ResourceBundle.getBundle("summer")
  def getValue(key:String) ={
    summer.getString(key)
  }

}
