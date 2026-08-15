package com.simbiocreacion.resource.dto;

// Vista pública de un usuario: solo datos seguros para exponer en el perfil público (sin email/role).
public record UserPublicDto(String id, String name, String firstName, String lastName, String pictureUrl, Integer score) { }
