
// https://github.com/once-ler/hl7parser-json/blob/master/include/hl7parser/seg_pv1.h

/*
  MSH|^~\&|AccMgr|1|||20050112154645||ADT^A03|59912415|P|2.3||| EVN|A03|20050112154642||||| 
  PID|1||10006579^^^1^MRN^1||DUCK^DONALD^D||19241010|M||1|111^DUCK ST^^FOWL^CA^999990000^^M|1|8885551212|8885551212|1|2||40007716^^^AccMgr^VN^1|123121234|||||||||||NO 
  PV1|1|I|IN1^214^1^1^^^S|3||IN1^214^1|37^DISNEY^WALT^^^^^^AccMgr^^^^CI|||01||||1|||37^DISNEY^WALT^^^^^^AccMgr^^^^CI|2|40007716^^^AccMgr^VN|4||||||||||||||||1|||1||P|||20050110045253|20050112152000|3115.89|3115.89|||
*/

case class PersonName
(
  id: Option[String],
  lastName: Option[String],
  firstName: Option[String],
  assigningAuthority: Option[String]
)

case class PersonLocation
(
  pointOfCare: Option[String],
  facility: Option[String]
) 

case class PatientVisit
(
  patientClass: Option[String],
  assignedPatientLocation: Option[PersonLocation],
  admissionType: Option[String],
  attendingDoctor: Option[PersonName],
  referringDoctor: Option[PersonName],
  hosipitalService: Option[String],
  reAdmissionIndicator: Option[String],
  dischargeDisposition: Option[String],
  admitDateTime: Option[String],
  dischargeDateTime: Option[String],
  visitIndicator: Option[String]
)
