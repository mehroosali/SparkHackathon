package org.inceptez.hack
//3.24
class allmethods extends Serializable {

  def remspecialchar( str : String ) : String = {
    return str.replaceAll( "[^a-zA-Z0-9]", " " );
  }
}
