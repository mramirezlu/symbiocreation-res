package com.simbiocreacion.resource.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;

@Document
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Symbiocreation {

    @Id
    private String id;
    private String name;

    private String place;
    private Date dateTime; // Java 8 date types do not serialize properly :(
    private String timeZone;
    private Boolean hasStartTime;

    private String description;
    private String infoUrl;
    private List<String> tags;
    private List<String> extraUrls;
    private List<String> sdgs;

    private String imgPublicId; // portada (public_id de Cloudinary, subida desde el navegador)

    private Date creationDateTime;
    private Date lastModified;
    private boolean enabled;
    private String visibility;

    private List<Participant> participants;
    private List<Node> graph;
    private Integer nParticipants;
}
