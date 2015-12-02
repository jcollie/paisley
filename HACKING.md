

## Implementation notes

_Database names_

 * names can contain slashes; Paisley automatically encodes these

_Document ids_

 * document ids can contain slashes.  Paisley automatically encodes these,
   except for some specific cases where couchdb would 301 redirect anyway:
   <Kxepal> thomasvs: both. it encoded as %2f, but there are exceptions for _design/ and _local/ for nicer urls
   <Kxepal> but they too supports %2f encoding
   <Kxepal> other doc ids must have slash encoded or router failed to guess where document id ends and attachment name starts
   <Kxepal> oh, slashes encoding for attachment names is also optional

