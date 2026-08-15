> Cambios del **backend** (`symbiocreation-res`). Los cambios del **frontend** están en `symbiocreation-ui-fork/CHANGELOG.md`.

# 14/12/25 mramirez
- **LlmService**: Corregido `IndexOutOfBoundsException` cuando una symbiocreation tiene menos de 3 ideas
- **LlmService**: Corregido `NullPointerException` en templates cuando `title` o `description` son null
- **AnalyticsController**: Corregido `NullPointerException` cuando `user.getScore()` es null
- **LlmService**: Convertidas llamadas bloqueantes a OpenAI a operaciones reactivas con `Mono.fromCallable()` y `Schedulers.boundedElastic()`
- **LlmService**: Agregado manejo de errores con `onErrorResume()` y logging en todos los metodos de IA
- **LlmService**: Agregada validacion de respuestas null del LLM
- **LlmService**: Agregada sanitizacion de inputs para prevenir prompt injection
- **LlmService**: Mensaje informativo cuando no hay ideas suficientes para generar sugerencias de IA
- **application.properties**: Configuracion de conexion a MongoDB local
- **application.properties**: Configuracion de API key de OpenAI

# 01/06/26 mramirez
- **LlmService**: Migrada generación de imágenes de DALL-E 3 (deprecado) a gpt-image-1 mediante llamada directa a la API de OpenAI con WebClient, debido a incompatibilidad con Spring AI 1.0.0-M2
- **SymbiocreationController**: Corregido endpoint de imagen para retornar `Mono<ResponseEntity<byte[]>>` en vez de `ResponseEntity<Mono<Resource>>`, con manejo de respuesta vacía (404)
- **SymbiocreationController**: Agregado filtro por nombre en exploración de symbiocreations públicas

# 11/07/26 mramirez
Revisión de la feature "Obtener ideas con IA" (ver `docs/revision-obtener-ideas-ia.md`):
- **IdeaAiResponse / LlmService**: Agregado campo `placeholder` al DTO para distinguir mensajes informativos ("se necesitan más ideas") de ideas reales; los sentinels se marcan con `placeholder = true`.

Nueva funcionalidad "Busco inspiración":
- **LlmService / ILlmService**: Nuevo método `getInspirationIdeasFromLlm` que genera 3 ideas basadas solo en el tema de la sesión (`name` + `description`), sin requerir ideas existentes. Si la sesión no tiene tema, devuelve un aviso con `placeholder = true`.
- **SymbiocreationController**: Nuevo endpoint `GET /symbiocreations/{id}/getInspirationFromAI` (sirve tanto para nodos hoja como para grupos).
- **LlmService**: La inspiración fuerza español en las ideas generadas (`USER_QUERY_INSPIRATION_TEMPLATE`), independientemente del idioma del tema.

Revisión de la feature "Generar imagen con IA":
- **LlmService**: Traduce el error de OpenAI a estados HTTP distinguibles (`503` sin cuota · `422` políticas de contenido · `502` genérico) vía `onErrorMap`/`mapImageError`, en lugar de colapsar todo en `Mono.empty()`→404.
- **LlmService**: Agregada directiva al prompt de generación de imagen (`IMAGE_NO_TEXT_DIRECTIVE`) para que el modelo no incluya texto/letras/números ni tipografía y produzca una ilustración limpia sin caracteres escritos (gpt-image-1 tiende a renderizar texto ilegible).
- **LlmService / prompts**: Agregado brief de dirección de arte (`prompts/system-for-image.md`, inyectado con `@Value` y cacheado en `@PostConstruct`) que define el lenguaje visual SIMBIO. El prompt de imagen se ensambla como: brief + idea (título/descripción) + directiva de no-texto.
- **SymbiocreationController / SymbiocreationService / SymbiocreationRepository**: El listado público del Explore ("Todas") ahora se ordena por **fecha de creación (`creationDateTime`) descendente** en vez de `lastModified`. Se renombró `findByVisibilityOrderByLastModifiedDesc` → `findByVisibilityOrderByCreationDateTimeDesc` y se agregó `sort` por `creationDateTime` desc a la variante de búsqueda por nombre.
- **SymbiocreationRepositoryImpl / Controller / Service**: El listado público del Explore ahora se resuelve con una **consulta dinámica** (`findPublicFiltered` / `countPublicFiltered`, con `ReactiveMongoTemplate` + `Criteria`) que combina nombre (opcional) y **rango de fecha de creación** (`from`/`to` en epoch millis, ambos opcionales): solo `from` → desde esa fecha en adelante; solo `to` → hasta esa fecha; ambos → entre ambas. Los endpoints `getAllPublic/{page}` y `countPublic` reciben `from`/`to`; **`countPublic` ahora también respeta el nombre** (antes lo ignoraba). Se eliminaron los métodos derivados obsoletos `findByVisibilityOrderByCreationDateTimeDesc` y `findByVisibilityAndNameContainingIgnoreCase`.
- **SymbiocreationController / Service / RepositoryImpl**: Nuevo endpoint `GET /symbiocreations/getPublicRanked?sort=&name=&limit=` para el ranking de simbios públicas del frontpage. `sort`: `ideas` (cantidad de ideas DESC), `new` (fecha de creación DESC) o `collaborators` (cantidad de participantes DESC). El ranking se computa en la app (`getPublicRanked` + `findAllByVisibilityFull`) porque cantidad de ideas/colaboradores son derivados; el grafo se descarta del payload tras contar. El controller usa `flatMapSequential` para preservar el orden. Nota de rendimiento: carga las públicas con grafo para contar ideas (patrón similar a `getTopSymbiocreations`); considerar cachear o almacenar `nIdeas` si crece el volumen.

# 28/07/26 mramirez
- **SymbiocreationController / Service / ISymbiocreationService**: `getPublicRanked` ahora acepta **rango de fecha de creación** (`from`/`to`, opcionales) y **`page`** para paginar el ranking. Firma: `getPublicRanked(name, sort, limit, page, from, to)`. Usado por "Explora Simbios" de Mi Perfil; el total del paginador viene de `countPublic`.
- **SymbiocreationController / SymbiocreationRepository**: "Mis Simbios" (`getMine`) ahora se ordena por **`creationDateTime` descendente** a través de todas las páginas, aplicado vía `Pageable` (se quitó el `sort` del `@Query` de `findAllByUser`).

# 29/07/26 mramirez
- **UserController / UserPublicDto**: Nuevo endpoint `GET /users/{id}/public` con **vista pública** del usuario (sin email/role/isGridViewOn), para perfiles públicos.
- **SymbiocreationController / Service / Repository**: Nuevos endpoints del perfil público: `getPublicOfUser/{userId}/{page}` (simbios públicas donde participa el usuario, sin grafo, paginado) y `countPublicOfUser/{userId}`.
- **SymbiocreationController / UserService / IUserService** (perf): `completeUsers` resuelve los participantes con **una sola consulta por lote** (`findAllById`) en vez de un `findById` por participante (elimina el N+1). Beneficia a `getMine`, Explore y `getPublicRanked`.
